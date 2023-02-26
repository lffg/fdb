# TODO

- [ ] Save `PAGE_SIZE` and `IDENTIFIER_SIZE` in the header. Also, parameterize
      them for testing purposes.

# Ideia: Dirty pages?

Como identificar páginas modificadas?

- O pager pode manter um hash map mantendo o metadado de quais páginas foram
  alteradas.

  - Mas como ele saberia que a página foi modificada?
    - Bem, se ele for o _dono_ das páginas, somente ele poderia conceder acesso
      de escrita usando uma closure... Assim, o usuário passa a página para um
      método genérico `edit`.

- Todavia, nesse caso, o `pager` deve estar ciente sobre transactions.
- A transaction pode enviar um "commit" ao Pager, que então irá triggar o flush
  geral e, se necessário, falhar. Assim, devolve-se o status de erro à
  transaction, que pode compensar da forma adequada.

### aka.

No BD temos a noção de páginas 'dirty', que foram alteradas e portanto
potencialmente devem ser flush-adas para o arquivo. Right? Right.

Todavia, eu não acho que seria muito interessante manter o bool is_dirty nas
próprias páginas... Já que teoricamente quem precisa dessa informação é apenas o
Pager (buffer pool manager, seja lá como você queira chamá-lo).

Mas então como garantir que o pager sempre irá saber que uma página está "dirty"
após ela ter sido modificada?

Well, talvez seja possível criar um método genérico no Pager que recebe o ID da
página e aceita uma closure que recebe por parâmetro uma ref mutável à página.
Assim, se a página for modificada, o Pager terá certeza disso, e vai poder
manter essa info e dar o flush quando for mais conveniente.

Talvez a implementação de controle de concorrência e transações traga alguma
implicação aqui, mas a priori isso não me parece _tão_ problemático...
