# nsq-http-polling
This is an NSQ client that reads the specified topic/channel and re-publishes as Http long-polling

```shell
# build
go build .

# startup it
./http_polling -nsqd-tcp-address :4150

# wait topic=foo
curl -vs 'http://127.0.0.1:4152/?topic=foo&channel=bar'

# publish a message
curl -d "hello" 'http://127.0.0.1:4151/pub?topic=foo'
```
