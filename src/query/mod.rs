use datafusion::logical_plan::Expr;

pub fn column_sort_expr_desc(column: String) -> Expr {
    Expr::Sort {
        expr: Box::new(Expr::Column(column)),
        asc: false,
        nulls_first: true,
    }
}

pub fn column_sort_expr_asc(column: String) -> Expr {
    Expr::Sort {
        expr: Box::new(Expr::Column(column)),
        asc: true,
        nulls_first: true,
    }
}

#[cfg(test)]
pub mod test;

pub mod graphql;
pub mod rest;
pub mod sql;
