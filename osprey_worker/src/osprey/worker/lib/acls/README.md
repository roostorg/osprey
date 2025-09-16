# Local Development and Testing

## Manual Testing

Local ACL assignments are stored in `dev_acl_assignments.json`. By default, `SUPER_USER` is enforced unless overwritten in this file.

```json
{
    "local-dev@localhost": {
        "roles": [
            "SUPER_USER"
        ]
    }
}
```


This should definitely be pluggy'd!
