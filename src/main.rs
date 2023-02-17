use std::{
    fs::OpenOptions,
    io::{self, Read, Seek, SeekFrom, Write},
};

#[derive(Debug)]
struct Record {
    is_deleted: bool,
    white_elo: u16,
    black_elo: u16,
    white_name: String,
}

impl Record {
    fn serialize(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write(&[u8::from(self.is_deleted)])?;
        out.write(&self.white_elo.to_le_bytes())?;
        out.write(&self.black_elo.to_le_bytes())?;

        // white_name
        out.write(&u16::try_from(self.white_name.len()).unwrap().to_le_bytes())?;
        out.write(self.white_name.as_bytes())?;

        Ok(())
    }

    fn deserialize(src: &mut dyn Read) -> io::Result<Self> {
        let mut buf8 = [0_u8; 1];
        let mut buf16 = [0_u8; 2];

        src.read_exact(&mut buf8)?;
        let is_deleted = u8_to_bool(buf8[0])?;

        src.read_exact(&mut buf16)?;
        let white_elo = u16::from_le_bytes(buf16);
        src.read_exact(&mut buf16)?;
        let black_elo = u16::from_le_bytes(buf16);

        src.read_exact(&mut buf16)?;
        let white_name_len: usize = u16::from_le_bytes(buf16).try_into().unwrap();

        let mut white_name_bytes = vec![0_u8; white_name_len];
        src.read_exact(&mut white_name_bytes)?;
        let white_name = String::from_utf8(white_name_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 string"))?;

        Ok(Record {
            is_deleted,
            white_elo,
            black_elo,
            white_name,
        })
    }
}

#[derive(Debug)]
struct Database {
    record_count: u32,
    records: Vec<Record>,
}

impl Database {
    fn serialize(&self, out: &mut dyn Write) -> io::Result<()> {
        out.write(&self.record_count.to_le_bytes())?;
        for record in &self.records {
            record.serialize(out)?;
        }
        Ok(())
    }

    fn deserialize(src: &mut dyn Read) -> io::Result<Self> {
        let mut buf32 = [0_u8; 4];
        src.read_exact(&mut buf32)?;
        let record_count = u32::from_le_bytes(buf32);

        let mut records = Vec::with_capacity(record_count.try_into().unwrap());
        for _ in 0..record_count {
            records.push(Record::deserialize(src)?);
        }

        Ok(Database {
            record_count,
            records,
        })
    }
}

fn main() -> io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("ignore/my-db")?;

    // optional
    file.seek(SeekFrom::Start(0))?;

    let db = Database {
        record_count: 2,
        records: vec![
            Record {
                is_deleted: false,
                white_elo: 7,
                black_elo: 9,
                white_name: "Luiz".into(),
            },
            Record {
                is_deleted: false,
                white_elo: 13,
                black_elo: 15,
                white_name: "Edu".into(),
            },
        ],
    };
    db.serialize(&mut file)?;

    // rewind after write, read it again for no reason
    file.seek(SeekFrom::Start(0))?;
    let my_db = Database::deserialize(&mut file)?;
    dbg!(my_db);

    Ok(())
}

fn u8_to_bool(byte: u8) -> io::Result<bool> {
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "invalid bool")),
    }
}
