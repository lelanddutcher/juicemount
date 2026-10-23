package catalog

import (
    "context"
    "database/sql"
)

func checkDeliveryOrder(ctx context.Context,q queryer) error {
    var missing int64
    if err:=q.QueryRowContext(ctx,`SELECT count(*) FROM byte_policy_events b WHERE NOT EXISTS(SELECT 1 FROM collection_order o WHERE o.byte_policy_sequence=b.sequence)`).Scan(&missing);err!=nil{return err}
    if missing!=0{return corrupt("byte policy order coverage")}
    var sequence int64
    err:=walkRows(ctx,q,`SELECT byte_policy_sequence FROM collection_order WHERE byte_policy_sequence IS NOT NULL ORDER BY ordinal`,func(rows *sql.Rows) error {
        var current int64
        if err:=rows.Scan(&current);err!=nil{return err}
        sequence++
        if current!=sequence{return corrupt("byte policy order")}
        return nil
    })
    if err!=nil{return err}
    var count int64
    if err:=q.QueryRowContext(ctx,`SELECT count(*) FROM byte_policy_events`).Scan(&count);err!=nil{return err}
    if sequence!=count{return corrupt("byte policy order count")}
    return nil
}
