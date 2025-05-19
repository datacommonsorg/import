## Using protos in spanner

The protos to be used in spanner are defined in `storage.proto`.

To use them spanner, follow the following steps:

### Create a proto descriptor

```shell
protoc --include_imports --descriptor_set_out=storage.pb storage.proto
```

### Update `bundle.ddl`

Update `bundle.ddl` if any protos were added, removed or updated.

### Upload proto descriptor to spanner

```shell
gcloud spanner databases ddl update DATABASE_ID --instance=INSTANCE_ID \
  --ddl-file=bundle.ddl \
  --proto-descriptors-file=storage.pb
```

`storage.pb` is a binary file so we don't commit it to the repo. 
Delete it after you've uploaded it to spanner.

Example usage:

```shell
gcloud spanner databases ddl update dc_graph_5 --instance=dc-kg-test \
  --ddl-file=bundle.ddl \
  --proto-descriptors-file=storage.pb
```

### Other info

---

Reference: https://cloud.google.com/spanner/docs/reference/standard-sql/protocol-buffers

---

Ensure that the package name and message name used in the DDL
(e.g. `org.datacommons.proto.Observations`) 
exactly match what's in `storage.proto` and the generated bundle.

---

In the spanner console, the proto field is displayed as base64 encoded strings.
To view the actual proto, copy the string and use the `./decode_proto.sh` script.

Usage:

```shell
./decode_obs.sh <base64_string>
```

Example:

```shell
./decode_obs.sh "Cg4KBzE5NDgtMTASAzEuMA=="
```

---

View DDL:

```shell
gcloud spanner databases ddl describe dc_graph_5 --instance=dc-kg-test
```