# elasticsearch-6.2.4

> For the elasticsearch 6.2.4 version

## Rest High Level Client

### Index Create

- createIndexSync & createIndexAsync

### Index Delete

- deleteIndexSync & deleteIndexAsync

### Index Close

- closeIndexSync & closeIndexAsync

### Index Open

- openIndexSync & openIndexAsync


`Sync后缀：为同步方法，调用后阻塞等待结果返回；若请求发生4xx or 5xx错误，则会抛出IoException | ElasticsearchException`

`Async后缀：为异步方法，调用会后立刻返回，通过listener回调监听返回结果（注：异步请求不能用try resource关闭client，需等待一段时间，否则在client关闭后异步请求无法到达）`