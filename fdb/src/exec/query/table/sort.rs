use std::{cmp::Ordering, collections::VecDeque, fmt, path::PathBuf};

use async_trait::async_trait;
use tokio::fs::OpenOptions;
use tracing::{instrument, trace};

use crate::{
    catalog::{object::TableObject, page::PageId, record::simple_record::SimpleRecord},
    error::{DbResult, Error},
    exec::{
        query::{table::linear_scan::LinearScan, Query},
        values::{SchematizedValues, Values},
    },
    io::disk_manager::DiskManager,
    util::io::{SerdeCtx, Size},
    Db,
};

// TODO: Review this code. It's currently HORRIFIC.

// TODO: Table-locking. Current locks -> latches.
//       Currently, the database implementation assumes that users won't perform
//       concurrent writes. This may not actually be the case, which might
//       corrupt the database file.

/// Sort options.
pub struct SortOpts<'a> {
    /// The maximum number of page sizes that should be allocated while loading
    /// pages from the disk.
    ///
    /// Must be AT LEAST `ways * 2 + 1`.
    pub mem_pages_limit: usize,
    /// Number of sorting-ways, i.e., the number of "cursors" to be used in the
    /// merging process.
    ///
    /// `ways * 2` files will be created as "scratch space".
    ///
    /// Minimum is two.
    ///
    /// Notice that `way` is NOT to be confused with `lane`. The number of lanes
    /// is given be `ways * 2`.
    pub ways: u16,
    /// The name of the columns to be ordered by.
    pub order_by: OrderBy<'a>,
}

pub type OrderBy<'a> = &'a [(&'a str, OrderByDirection)];

/// Order by direction.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OrderByDirection {
    Asc,
    Desc,
}

/// A sort query.
pub struct Sort<'a> {
    table: &'a TableObject,
    opts: SortOpts<'a>,
}

#[async_trait]
impl Query for Sort<'_> {
    type Item<'a> = SimpleRecord<'static, SchematizedValues<'static>>;

    #[instrument(name = "TableSort", level = "debug", skip_all)]
    async fn next<'a>(&mut self, db: &'a Db) -> DbResult<Option<Self::Item<'a>>> {
        external_merge_sort(db, &self.table, &self.opts).await?;
        todo!();
    }
}

impl<'a> Sort<'a> {
    pub fn new(table: &'a TableObject, opts: SortOpts<'a>) -> Sort<'a> {
        Self { table, opts }
    }
}

type RecordData = SchematizedValues<'static>;
type Record = SimpleRecord<'static, RecordData>;

/// Perform external merge sort from the given `table`'s records.
// TODO: Return final file.
#[instrument(level = "trace", skip_all)]
async fn external_merge_sort(db: &Db, table: &TableObject, opts: &SortOpts<'_>) -> DbResult<()> {
    assert!(
        opts.mem_pages_limit >= opts.ways as usize * 2 + 1,
        "mem pages limit must be at least `ways * 2 + 1`"
    );

    let mut aux_buf = vec![0; db.page_size() as usize];
    let aux_buf = &mut buff::Buff::new(&mut aux_buf);
    // -1 because the buffer above occupies 1 page size.
    let memory_limit = opts.mem_pages_limit - 1;

    let cmp_fn = build_cmp_fn(table, &opts.order_by)?;

    // Allocate scratch files to perform the sorting...
    //
    // TODO: Maybe this method should already return the queue...
    let all_lanes = FileLanes::allocate_for_ways(db.page_size(), opts.ways).await?;

    // Allocate a queue to dynamically determine the "lane groups".
    //
    // I.e., if the user provided `ways` as `3`, then the database allocates `6`
    // files (i.e., lanes) to use as a sorting scratch space. Each "lane group"
    // has length `ways`. Also, each "group" (i.e., slice in the queue) may be
    // called as an "way group", or "ways".
    let mut lanes_queue: VecDeque<FileLanes> = all_lanes.chunks_for_ways().collect();

    let mut first_lanes = lanes_queue.pop_front().unwrap();
    distribute(
        db,
        table,
        memory_limit as u16,
        &mut first_lanes,
        &cmp_fn,
        aux_buf,
    )
    .await?;
    lanes_queue.push_back(first_lanes);

    let mut debug_saver = 0;

    loop {
        debug_saver += 1;
        if debug_saver >= 15 {
            panic!("woo! from top");
        }

        trace!("merging...");

        let mut out_ways = lanes_queue.pop_front().unwrap();
        let mut in_ways = lanes_queue.pop_front().unwrap();
        let res = merge_ways(
            db,
            table,
            opts.ways,
            memory_limit as u16,
            &mut in_ways,
            &mut out_ways,
            &cmp_fn,
            aux_buf,
        )
        .await?;

        if res {
            tracing::warn!("done");
            break;
        }

        // `in` is `out` now. Must rewind.
        in_ways.reset_all_lanes();
        lanes_queue.push_back(in_ways);
        // (...) and `out` is now `in`.
        lanes_queue.push_back(out_ways);
    }

    Ok(())
}

/// Distribution phase of the external merge sort.
#[instrument(level = "trace", skip_all)]
async fn distribute<'a>(
    db: &Db,
    table: &'a TableObject,
    mem_pages_limit: u16,
    lanes: &mut FileLanes,
    cmp_fn: &CmpFn<'a>,
    aux_buf: &mut buff::Buff<'_>,
) -> DbResult<()> {
    let mut linear_scan_query = LinearScan::new(table);
    let mut finished = false;

    let mem_limit = mem_pages_limit as usize * db.page_size() as usize;

    let mut prev_record: Option<Record> = None;

    while !finished {
        let mut record_data: Vec<RecordData> = Vec::new();
        let mut current_group_size = 0_usize;

        // Gets the current lane.
        let mut current_lane = lanes.current_and_advance_wrapping();
        current_lane.new_group();

        // Keep reading records until there isn't more memory available.
        //
        // One needs to keep track of memory manually here since `LinearScan`
        // returns each record in the table, without any kind of page
        // information.
        //
        // Furthermore, currently stored records and pages may have trailing
        // padding, which shouldn't be considered by the sort implementation.
        'read: while current_group_size < mem_limit {
            // Reads the next record.
            let maybe_next_record = next_record(
                db,
                &mut linear_scan_query,
                current_group_size,
                &mut prev_record,
            )
            .await?;

            // If the table sequence was exhausted, one stops iterating.
            let Some(mut record) = maybe_next_record else {
                trace!("finishing read all records in heap sequence");
                finished = true;
                break 'read;
            };

            // Ignore deleted records.
            if record.is_deleted() {
                continue;
            }

            // SAFETY: This record is going to be inserted at the last position
            // in the scratch file. Hence, one's not going to form any holes.
            unsafe { record.clear_padding() };

            // Tries to fit the new record into the available memory.
            let record_size = record.as_data().size();

            current_group_size += record_size as usize;
            if current_group_size > mem_limit {
                trace!(
                    ?current_group_size,
                    ?mem_limit,
                    "no more memory available in group"
                );
                current_group_size = 0;
                // Otherwise, store it so one may get it in the next iteration.
                prev_record = Some(record);
                break 'read;
            }

            let data = record.into_data().into_owned();
            record_data.push(data);
        }

        trace!(
            record_count = record_data.len(),
            ?current_group_size,
            "sorting to flush"
        );

        record_data.sort_by(|a, b| cmp_fn(a.as_values(), b.as_values()));

        dump_records_into_lane(db, table, &mut current_lane, record_data, aux_buf).await?;
    }

    Ok(())
}

/// Merges `in_ways` into `out_ways`.
///
/// Returns true if it only wrote to a single lane, indicating the end of the
/// merging process.
#[instrument(level = "trace", skip_all)]
async fn merge_ways<'a>(
    db: &Db,
    table: &'a TableObject,
    ways: u16,
    mem_pages_limit: u16,
    in_ways: &mut FileLanes,
    out_ways: &mut FileLanes,
    cmp_fn: &CmpFn<'a>,
    aux_buf: &mut buff::Buff<'_>,
) -> DbResult<bool> {
    debug_assert_eq!(in_ways.lanes.len(), ways as usize);

    let mut wrote_to = 0_u16;

    let pages_per_way = ways;
    let flush_buffer_pages = mem_pages_limit - ways;
    let flush_buffer_size = flush_buffer_pages as u32 * db.page_size() as u32;
    trace!(
        ?mem_pages_limit,
        ?ways,
        ?pages_per_way,
        ?flush_buffer_pages,
        "merge_ways memory data"
    );

    // - Lanes
    //     - Groups
    //         - Records
    //
    // 'groups: ENQUANTO HOUVER GRUPOS:
    //     lane = out_ways.current_and_advance_wrapping();
    //     lane.start_group();
    //
    //     let lanes_records_queues;
    //
    //     let written_pages = 0;
    //     let written_pages_records = Vec::new();
    //
    //     'page: ENQUANTO HOUVER PÁGINAS NO GRUPO:
    //          CONSUMED_GROUP = false
    //          'lane: PARA CADA LANE:
    //              se `written_pages` estiver CHEIO.
    //                  ESCREVER O CACHE. SALVAR.
    //                  RESET NO TAMANHO DE WRITTEN_PAGES.
    //                  ADICIONAR NOVA PÁGINA NO GRUPO (OUT) QUE JÁ EXISTE.
    //              se estiver faltando elemento na fila para esta lane...
    //                  se não tiver próximo grupo:
    //                      continuar para próxima 'lane.
    //                  CONSUMED_GROUP = true
    //                  se não tiver mais elementos no grupo:
    //                      remover o grupo;
    //                      continuar para próxima 'lane.
    //                  CARREGAR_ELEMENTOS_PARA_ESTA_LANE.
    //                  PERFORMAR >>>MERGE<<<
    //                  ADICIONAR MENOR ELEMENTO NO
    //          'lane fim.
    //          caso contrário,
    //              se !CONSUMED_GROUP:
    //                  break 'groups (acabaram os grupos).
    //              continue 'groups (novo grupo)
    //

    // While there are groups:
    'groups: loop {
        let mut current_lane = out_ways.current_and_advance_wrapping();
        current_lane.new_group();

        let mut records_to_write = (0, Vec::<SchematizedValues>::new());

        let mut lanes_ctl: Vec<_> = in_ways
            .lanes
            .iter_mut()
            .map(|lane| (lane, VecDeque::<SchematizedValues>::new()))
            .collect();

        // While there are pages in the current group:
        'page: loop {
            let mut loop_ctl_consumed_group = false;
            let mut loop_ctl_consumed_pages = false;

            trace!("reading pages for each group in all lanes");

            // Control for merging sorting.
            // Lane index and maximum element found.
            let mut smallest_record: Option<(usize, SchematizedValues)> = None;

            // Keep repeating over lanes.
            'lane: for lane_index in 0..lanes_ctl.len() {
                let (lane, unconsumed_records) = &mut lanes_ctl[0];
                let unconsumed_records_len = unconsumed_records.len();

                trace!(
                    ?lane_index,
                    ?unconsumed_records_len,
                    "trying to read more records from lane"
                );

                if unconsumed_records_len == 0 {
                    trace!(
                        ?lane_index,
                        "consumed all records for lane, trying to fetching more"
                    );

                    let Some(page_group) = lane.groups.front_mut() else {
                        trace!(?lane_index, "no more GROUPS to be consumed in lane");
                        continue 'lane;
                    };
                    loop_ctl_consumed_group = true;

                    let maybe_consumed_group_part = page_group.consume(pages_per_way as usize);
                    let Some((first_page_id, group_pages_record_counts)) = maybe_consumed_group_part else {
                        trace!(?lane_index, "no more PAGES to be consumed in lane's group");
                        lane.groups.pop_front();
                        continue 'lane;
                    };

                    trace!(
                        ?lane_index,
                        page_count = ?group_pages_record_counts.len(),
                        "loading more data"
                    );

                    let mut page_id = first_page_id;
                    for record_count in group_pages_record_counts {
                        aux_buf.seek(0);
                        trace!(?lane_index, ?page_id, ?record_count, "reading page");
                        lane.disk_manager
                            .read_page(page_id, aux_buf.get_mut())
                            .await?;
                        for _ in 0..record_count {
                            let record = SchematizedValues::deserialize(aux_buf, &table.schema)?;
                            unconsumed_records.push_back(record);
                        }
                        page_id += 1;
                    }
                }

                // Perform the actual merging.
                {
                    trace!(?lane_index, "performing merge...");
                    let Some(current_record) = unconsumed_records.front() else {
                        trace!(?lane_index, "no records in group, skipping");
                        // If there isn't any records to be consumed in the
                        // group, skip it.
                        continue 'lane;
                    };
                    // Marks this iteration as a "useful" one. One doesn't want
                    // to stop probing for records, yet.
                    loop_ctl_consumed_pages = true;

                    // If there isn't a minimum recorded record, yet:
                    let Some((_, min)) = &mut smallest_record else {
                        // SAFETY: Checked above (i.e., `continue`d if `None`).
                        smallest_record = Some((lane_index, unconsumed_records.pop_front().unwrap()));
                        trace!(?lane_index, "consumed FIRST minimum (default)");
                        continue 'lane;
                    };

                    // If `current_record` is smaller than the current smallest
                    // element, one updates the marker.
                    let current_values = current_record.as_values();
                    let min_values = min.as_values();
                    if cmp_fn(current_values, min_values).is_lt() {
                        let new_min = unconsumed_records.pop_front().unwrap();
                        let (old_min_lane_index, old_min) =
                            smallest_record.replace((lane_index, new_min)).unwrap();
                        debug_assert_ne!(
                            old_min_lane_index, lane_index,
                            "can't take minimum from the same lane"
                        );
                        trace!(?lane_index, "consumed NEW minimum (default)");
                        // Put the previous-min back:
                        lanes_ctl[old_min_lane_index].1.push_front(old_min);
                    }

                    // Must have minimum by now.
                    let (_, record) = smallest_record.take().unwrap();

                    tracing::error!("new smallest {:#?}", &record);

                    let (size, list) = &mut records_to_write;
                    *size += record.size();
                    if *size as u32 > flush_buffer_size {
                        trace!("flushing page to out");
                        let list = std::mem::take(list);
                        *size = 0;
                        dump_records_into_lane(db, table, &mut current_lane, list, aux_buf).await?;
                    }

                    list.push(record);
                    continue 'page;
                }
            }

            if !loop_ctl_consumed_group {
                trace!("all lanes didn't consume any pages in current groups, general exit");
                break 'groups;
            }

            if !loop_ctl_consumed_pages {
                trace!("finished lane group, going to the next");
                continue 'groups;
            }
        }
    }

    /*

    let mut debug_saver = 0; // <<<<<<>>>>>>>>>>>>>>><<<<
                             // Keep reading group pages until they are exhausted.
    'all_groups: loop {
        'group: loop {
            debug_saver += 1;
            if debug_saver >= 15 {
                panic!("woo! from merge_ways"); // <<<<<<<<<<
            }

            // Gets the current lane.
            let mut current_lane = out_ways.current_and_advance_wrapping();
            current_lane.new_group();

            let mut available_records = Vec::new();

            let mut lanes_ctl: Vec<_> = in_ways
                .lanes
                .iter_mut()
                .map(|lane| (lane, VecDeque::new()))
                .collect();

            // Takes `mem_pages_limit` pages, `pages_per_way` for each lane.
            'all_lanes: loop {
                let mut consumed_any_group = false;
                let mut consumed_any_record = false;

                trace!("trying to read pages for each lane group");
                // Control for merging sorting.
                // Lane index and maximum element found.
                let mut smallest_record: Option<(usize, SchematizedValues)> = None;

                'lane: for lane_index in 0..lanes_ctl.len() {
                    let (lane, unconsumed_records) = &mut lanes_ctl[0];
                    let unconsumed_records_len = unconsumed_records.len();

                    trace!(
                        ?lane_index,
                        ?unconsumed_records_len,
                        "trying to read lane group pages"
                    );

                    if unconsumed_records_len == 0 {
                        trace!(?lane_index, "consumed all records for lane, fetching more");

                        let Some(page_group) = lane.groups.front_mut() else {
                            trace!(?lane_index, "no more GROUPS to be consumed in lane");
                            if !consumed_any_group {
                                trace!("no more GROUPS to be consumed in all lanes");
                                break 'group;
                            }
                            continue 'lane;
                        };
                        consumed_any_group = true;

                        let maybe_consumed_group_part = page_group.consume(pages_per_way as usize);
                        let Some((first_page_id, group_pages_record_counts)) = maybe_consumed_group_part else {
                            trace!(?lane_index, "no more PAGES to be consumed in lane's group");
                            lane.groups.pop_front();
                            continue 'lane;
                        };

                        let mut page_id = first_page_id;
                        for record_count in group_pages_record_counts {
                            aux_buf.seek(0);
                            trace!(?lane_index, ?page_id, ?record_count, "reading page");
                            lane.disk_manager
                                .read_page(page_id, aux_buf.get_mut())
                                .await?;
                            for _ in 0..record_count {
                                unconsumed_records.push_back(SchematizedValues::deserialize(
                                    aux_buf,
                                    &table.schema,
                                )?);
                            }
                            page_id += 1;
                        }
                    }

                    // Perform the actual merging.
                    {
                        trace!(?lane_index, "performing merge...");
                        let Some(current_record) = unconsumed_records.front() else {
                            trace!(?lane_index, "no records in group, skipping");
                            // If there isn't any records to be consumed in the
                            // group, skip it.
                            continue 'lane;
                        };
                        // Marks this iteration as a "useful" one. One doesn't want
                        // to stop probing for records, yet.
                        consumed_any_record = true;

                        // If there isn't a minimum recorded record, yet:
                        let Some((_, min)) = &mut smallest_record else {
                            // SAFETY: Checked above (i.e., `continue`d if `None`).
                            smallest_record = Some((lane_index, unconsumed_records.pop_front().unwrap()));
                            trace!(?lane_index, "consumed FIRST minimum (default)");
                            continue 'lane;
                        };

                        // If `current_record` is smaller than the current smallest
                        // element, one updates the marker.
                        let current_values = current_record.as_values();
                        let min_values = min.as_values();
                        if cmp_fn(current_values, min_values).is_lt() {
                            let new_min = unconsumed_records.pop_front().unwrap();
                            let (old_min_lane_index, old_min) =
                                smallest_record.replace((lane_index, new_min)).unwrap();
                            debug_assert_ne!(
                                old_min_lane_index, lane_index,
                                "can't take minimum from the same lane"
                            );
                            trace!(?lane_index, "consumed NEW minimum (default)");
                            // Put the previous-min back:
                            lanes_ctl[old_min_lane_index].1.push_front(old_min);
                        }

                        // Must have minimum by now.
                        let min = smallest_record.take().unwrap();
                        available_records.push(min);
                    }
                }

                if available_records.len() > 0 {
                    trace!("consumed records, writing...");
                    continue 'group;
                } else {
                    trace!("no records consumed in group?");
                    break 'group;
                }
            }
        }
    }

        */

    /*
    'main: loop {


        let mut changed = false;
        let mut records: Vec<VecDeque<_>> = (0..ways).map(|_| VecDeque::new()).collect();

        'lane: for (i, lane) in in_ways.lanes.iter_mut().enumerate() {
            // Takes `pages_per_way` records.
            for _ in 0..pages_per_way {
                let Some(page_group) = lane.groups.front_mut() else {
                    trace!("no more GROUPS to be consumed");
                    continue 'lane;
                };
                let maybe_consumed_group_part = page_group.consume(pages_per_way as usize);
                let Some((first_page_id, group_page_counts)) = maybe_consumed_group_part else {
                    trace!("no more PAGES to be consumed");
                    continue 'lane;
                };

                changed = true;

                tracing::error!(?group_page_counts);

                let mut page_id = first_page_id;
                for group_page_count in group_page_counts {
                    aux_buf.seek(0);
                    lane.disk_manager
                        .read_page(page_id, aux_buf.get_mut())
                        .await?;
                    for _ in 0..group_page_count {
                        records[i]
                            .push_back(SchematizedValues::deserialize(aux_buf, &table.schema)?);
                    }
                    page_id += 1;
                }
            }
        }

        // If there isn't any more records to be consumed, one finishes.
        if !changed {
            break 'main;
        }

        let mut flat_records: Vec<_> = records.into_iter().flatten().collect();
        flat_records.sort_by(|a, b| cmp_fn(a.as_values(), b.as_values()));

        dump_records_into_lane(db, table, &mut current_lane, flat_records, aux_buf).await?;
        wrote_to += 1;

        // tracing::warn!("flat_records = \n{flat_records:#?}");

        /*
        let mut sorted_records = Vec::new();
        loop {
            tracing::warn!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            tracing::error!("{:#?}", records);
            tracing::warn!("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            let mut current_smallest_values_index = 0;
            let mut maybe_current_smallest_values = None;

            for i in 0..records.len() {
                let group = &mut records[i];

                if let Some(current_values) = group.pop_front() {
                    let Some(current_smallest_values) = maybe_current_smallest_values.take() else {
                        maybe_current_smallest_values = Some(current_values);
                        continue;
                    };

                    let cmp = cmp_fn(
                        current_values.as_values(),
                        current_smallest_values.as_values(),
                    );
                    if cmp.is_le() {
                        // Put the previous smallest back, if it exists.
                        records[current_smallest_values_index].push_front(current_smallest_values);

                        // Register the new smallest.
                        current_smallest_values_index = i;
                        maybe_current_smallest_values = Some(current_values);
                    }
                }
            }

            let Some(smallest) = maybe_current_smallest_values else {
                break;
            };

            sorted_records.push(smallest);
        }

        tracing::error!("{:#?}", sorted_records);

        dump_records_into_lane(db, table, &mut current_lane, sorted_records, aux_buf).await?;
        wrote_to += 1;*/
    }*/

    Ok(wrote_to == 1)
}

/// Dumps into the given lane.
///
/// Expects a lane group to be opened for this write.
#[instrument(level = "trace", skip_all)]
async fn dump_records_into_lane(
    db: &Db,
    table: &TableObject,
    current_lane: &mut FileLane,
    data: Vec<SchematizedValues<'_>>,
    aux_buf: &mut buff::Buff<'_>,
) -> DbResult<()> {
    // Write all records, grouping them into pages.
    let mut iter = data.into_iter().peekable();
    'group_writer: loop {
        let mut exhausted_records = false;

        let mut current_page_record_count = 0_u16;
        let mut current_page_size = 0_u32;

        // Normally, in this use case, this `seek` call would be
        // "unsafe" (i.e., risk database file corruption) since one
        // COULD write garbage into the page. However, since one's
        // calling `pad_end_bytes` afterwards, this operation is valid.
        // See @@ref1.
        aux_buf.seek(0);

        // Try to serialize as many records fit into a single page.
        let page_record_count = 'page_writer: loop {
            let Some(next_record_ref) = iter.peek() else {
                trace!("no more records to peek");
                exhausted_records = true;
                break 'page_writer current_page_record_count;
            };

            // If the size of the page being created would exceed the DB's
            // page size limit, one stops and flushes the page. It will be
            // read in the next iteration of the `'page_writer` loop.
            current_page_size += next_record_ref.size();
            if current_page_size > db.page_size() as u32 {
                trace!(
                    ?current_page_size,
                    page_memory_limit = ?db.page_size(),
                    "no more memory in page"
                );
                break 'page_writer current_page_record_count;
            }

            // SAFETY: Just checked above.
            let record = unsafe { iter.next().unwrap_unchecked() };
            record.serialize(aux_buf, &table.schema)?;

            current_page_record_count += 1;
        };

        if page_record_count > 0 {
            // Pads the end of the page with zeroes, as needed by @@ref1. ↑↑
            aux_buf.pad_end_bytes(0);

            let page_id = current_lane
                .current_group()
                .current_and_advance_with(page_record_count);

            trace!("writing page into group");
            current_lane
                .disk_manager
                .write_page(page_id, aux_buf.get())
                .await?;
        }

        if exhausted_records {
            trace!("finished lane group writes");
            break 'group_writer;
        }
    }

    Ok(())
}

/// Returns the next record in the sequence, or none if seq was exhausted.
///
/// This function is used by [`distribute`].
async fn next_record(
    db: &Db,
    linear_scan_query: &mut LinearScan<'_>,
    current_read_size: usize,
    prev_record: &mut Option<Record>,
) -> DbResult<Option<Record>> {
    let record = if let prev_record @ Some(_) = prev_record.take() {
        debug_assert_eq!(
            current_read_size, 0,
            "should only read `prev_record` when initializing new run"
        );
        prev_record
    } else {
        linear_scan_query.next(db).await?
    };
    Ok(record)
}

/// A cmp function.
type CmpFn<'a> = dyn Send + Sync + 'a + for<'v> Fn(&'v Values, &'v Values) -> Ordering;

/// Builds the cmp fn (for sorting).
fn build_cmp_fn<'a>(table: &'a TableObject, order_by: OrderBy<'a>) -> DbResult<Box<CmpFn<'a>>> {
    if order_by.len() != 1 {
        return Err(Error::ExecError(
            "fdb currently only supports order by with one field".into(),
        ));
    }

    for (col_name, _) in order_by {
        if table.schema.column(col_name).is_none() {
            return Err(Error::ExecError(format!(
                "column `{col_name}` doesn't exist on table {}",
                table.name
            )));
        }
    }

    Ok(Box::new(|a: &Values, b: &Values| -> Ordering {
        let col_name = order_by[0].0;
        let direction = order_by[0].1;

        // SAFETY: Column exists (as checked above).
        let a_val = unsafe { a.get(col_name).unwrap_unchecked() };
        let b_val = unsafe { b.get(col_name).unwrap_unchecked() };

        let cmp_result = a_val.cmp(&b_val);

        if direction == OrderByDirection::Asc {
            cmp_result
        } else {
            cmp_result.reverse()
        }
    }))
}

/// Represents a group of file lanes.
#[derive(Debug)]
struct FileLanes {
    way_count: u16,
    lanes: Vec<FileLane>,
    next: usize,
}

impl FileLanes {
    /// Returns a [`FileLanes`] with `way_count` lanes. Allocates two files for
    /// each way. Hence, `way_count * 2` files will be allocated by this
    /// constructor.
    async fn allocate_for_ways(page_size: u16, way_count: u16) -> DbResult<FileLanes> {
        let lanes_count = way_count * 2;
        let lanes_futures = (0..lanes_count).map(|id| FileLane::new(page_size, id));
        let lanes = futures_util::future::try_join_all(lanes_futures).await?;
        Ok(FileLanes {
            way_count,
            lanes,
            next: 0,
        })
    }

    /// Returns an iterator that produces [`FileLanes`] with `way_count`
    /// [`FileLanes`] on each.
    fn chunks_for_ways(self) -> impl Iterator<Item = FileLanes> {
        let way_count = self.way_count as usize;

        let mut lanes_for_chunk = Vec::with_capacity(way_count);
        let mut iter = self.lanes.into_iter();
        std::iter::from_fn(move || loop {
            let Some(lane) = iter.next() else {
                assert_eq!(lanes_for_chunk.len(), 0);
                break None;
            };
            lanes_for_chunk.push(lane);
            if lanes_for_chunk.len() == way_count {
                break Some(FileLanes {
                    way_count: way_count as u16,
                    lanes: std::mem::replace(&mut lanes_for_chunk, Vec::with_capacity(way_count)),
                    next: 0,
                });
            }
        })
    }

    /// Resets all lanes.
    fn reset_all_lanes(&mut self) {
        for lane in &mut self.lanes {
            lane.reset();
        }
    }

    /// Returns the current lane and advances the underlying counter so that the
    /// next call to `current` returns the next lane.
    fn current_and_advance_wrapping(&mut self) -> &mut FileLane {
        let len = self.lanes.len();

        let curr = &mut self.lanes[self.next];
        self.next = (self.next + 1) % len;
        curr
    }
}

/// Represents an external merge sort file "lane".
struct FileLane {
    /// The underlying disk manager.
    disk_manager: DiskManager,
    /// Each lane is composed by many "groups". This vector stores each group's
    /// information.
    groups: VecDeque<LaneGroup>,
}

impl FileLane {
    /// Constructs a new lane.
    async fn new(page_size: u16, path_id: u16) -> DbResult<FileLane> {
        let path = PathBuf::from(format!("ignore/fdb-sort-{path_id}"));
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await?;
        let disk_manager = DiskManager::from_file(path, file, page_size);
        Ok(FileLane {
            disk_manager,
            groups: VecDeque::new(),
        })
    }

    /// Resets the `FileLane`.
    fn reset(&mut self) {
        self.groups.clear();
    }

    /// Creates a new [`LaneGroup`] and appends it at the end of the
    /// [`FileLane`].
    fn new_group(&mut self) {
        // Computes the new group's `first_page_id` based on the previous one.
        let first_page_id = self
            .groups
            .back()
            .map(|group| group.current_page_id() + 1)
            .unwrap_or(PageId::FIRST);

        self.groups.push_back(LaneGroup {
            pages: VecDeque::new(),
            first_page_id,
        });
    }

    /// Returns the current group (i.e., the last one).
    fn current_group(&mut self) -> &mut LaneGroup {
        self.groups
            .back_mut()
            .expect("must call `new_group` before")
    }
}

impl fmt::Debug for FileLane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let path = self.disk_manager.path().display();
        f.debug_struct("FileLane")
            .field("disk_manager", &format!("<disk_manager @ `{path}`>",))
            .field("groups", &self.groups)
            .finish()
    }
}

/// Each lane is divided into groups. Groups are formed of database page tables.
///
/// ```text
/// [page | page | page ] [page | page]
/// |        |            \-----------/---------> Group
/// \--------|------------------------/---------> Lane
///          \----------------------------------> Page
/// ```
#[derive(Debug)]
struct LaneGroup {
    /// The amount of records in each page of the group.
    ///
    /// - Index 0 -> First page of the group.
    /// - Index 1 -> Second page of the group.
    /// - (etc)
    pages: VecDeque<u16>,
    /// The ID of the first page in the group.
    first_page_id: PageId,
}

impl LaneGroup {
    /// Returns the current [`PageId`]. After, increments the page count
    /// registering `record_count` records which MUST BE (by the caller) stored
    /// in the page identified by the returned [`PageId`].
    fn current_and_advance_with(&mut self, record_count: u16) -> PageId {
        let old = self.current_page_id();
        self.pages.push_back(record_count);
        old
    }

    /// Returns the number of pages in this group.
    fn page_count(&self) -> u32 {
        self.pages.len() as u32
    }

    /// Returns the current (i.e., last) [`PageId`] in the group.
    fn current_page_id(&self) -> PageId {
        self.first_page_id + self.page_count()
    }

    /// Consumes `n` pages, returning the iterator and the first page ID of the
    /// iterated sequence.
    fn consume(&mut self, n: usize) -> Option<(PageId, Vec<u16>)> {
        if self.pages.len() == 0 {
            return None;
        }
        let old_first = self.first_page_id;
        self.first_page_id += n as u32;
        let mut vec = Vec::new();
        for _ in 0..n {
            if let Some(page_count) = self.pages.pop_front() {
                vec.push(page_count);
            } else {
                break;
            }
        }
        Some((old_first, vec))
    }
}
