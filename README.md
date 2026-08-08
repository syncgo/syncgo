# syncgo

Golang implementation of [PGSync](https://pgsync.com/)

## DEMO

To run a little demo execute commands below 

```bash
make demo

# in another shell 
make generate_data

curl -X GET "http://localhost:9200/test_index/_search?pretty"
```
