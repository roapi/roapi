use datafusion::{
    logical_expr::expr::Sort,
    prelude::{Column, Expr},
};

pub fn column_sort_expr_desc(column: String) -> Sort {
    Sort {
        expr: Expr::Column(Column::from_name(column)),
        asc: false,
        nulls_first: true,
    }
}

pub fn column_sort_expr_asc(column: impl Into<String>) -> Sort {
    Sort {
        expr: Expr::Column(Column::from_name(column)),
        asc: true,
        nulls_first: true,
    }
}

pub mod graphql;
pub mod rest;
pub mod sql;
