use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl QueryResult {
    fn new_from_rows(rows: Vec<serde_json::Value>) -> Self {
        let mut result = QueryResult::default();

        // Extract column names from the first object (if available)
        if let Some(first_row) = rows.first() {
            result.columns = first_row
                .as_object()
                .expect("row is not an object")
                .keys()
                .cloned()
                .collect();
        }

        // Process each row to populate rows field
        for row in rows {
            let obj = row.as_object().expect("row is not an object");
            let row_values = result
                .columns
                .iter()
                .map(|column| {
                    let value = obj.get(column).unwrap_or(&serde_json::Value::Null);
                    match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => "null".to_string(),
                        _ => value.to_string(), // Handle arrays/objects as JSON strings
                    }
                })
                .collect::<Vec<_>>();
            result.rows.push(row_values);
        }

        result
    }
}

#[derive(serde::Deserialize)]
struct ErrorResponse {
    error: String,
    message: String,
}

#[derive(serde::Deserialize)]
struct SchemaField {
    name: String,
    nullable: bool,
    // data_type: TODO
}

#[derive(serde::Deserialize)]
struct Schema {
    pub fields: Vec<SchemaField>,
}

pub struct ROAPIUI {
    query: String,
    error: Option<String>,
    results: Option<QueryResult>,
    tables: Option<HashMap<String, Schema>>,

    query_response: Arc<Mutex<Option<Result<ehttp::Response, String>>>>,
    schema_response: Arc<Mutex<Option<Result<ehttp::Response, String>>>>,
    schema_fetching_message: Option<String>,

    show_schema: BTreeSet<String>,
    show_query_executing: bool,
    show_settings: bool,
    show_error: bool,
}

fn roapi_base() -> &'static str {
    #[cfg(target_arch = "wasm32")]
    return "";
    #[cfg(not(target_arch = "wasm32"))]
    return "http://localhost:8080";
}

impl Default for ROAPIUI {
    fn default() -> Self {
        Self::new()
    }
}

impl ROAPIUI {
    pub fn new() -> Self {
        Self {
            query: "".to_string(),
            tables: None,
            results: None,
            error: None,
            query_response: Arc::new(Mutex::new(None)),
            schema_response: Arc::new(Mutex::new(None)),
            schema_fetching_message: None,
            show_query_executing: false,
            show_settings: false,
            show_error: false,
            show_schema: BTreeSet::new(),
        }
    }

    fn http_execute_query(&mut self) {
        let url = format!("{}/api/sql", roapi_base());
        let request = ehttp::Request::post(url, self.query.as_bytes().to_vec());
        let download = self.query_response.clone();
        ehttp::fetch(request, move |result| {
            *download.lock().expect("failed to acquire lock") = Some(result);
        });
        self.show_query_executing = true;
    }

    fn http_fetch_schema(&mut self) {
        let url = format!("{}/api/schema", roapi_base());
        let request = ehttp::Request::get(url);
        let download = self.schema_response.clone();
        ehttp::fetch(request, move |result| {
            *download.lock().expect("failed to acquire lock") = Some(result);
        });
        self.schema_fetching_message = Some("Fetching schema...".to_string());
    }

    fn process_query_result(&mut self, result: Result<ehttp::Response, String>) {
        match result {
            Ok(response) => match response.status {
                200 => match response.json::<Vec<serde_json::Value>>() {
                    Ok(rows) => {
                        self.results = Some(QueryResult::new_from_rows(rows));
                        self.show_error = false;
                        self.error = None;
                    }
                    Err(e) => {
                        self.show_error = true;
                        self.error = Some(format!("Failed to parse query result: {e}"));
                    }
                },
                _ => {
                    self.show_error = true;
                    let code = response.status;
                    match response.json::<ErrorResponse>() {
                        Ok(resp) => {
                            self.error = Some(format!(
                                "Request failed with status: {code}, error: {}, message: {}",
                                resp.error, resp.message
                            ));
                        }
                        Err(_) => {
                            self.error = Some(format!("Request failed with status: {code}"));
                        }
                    }
                }
            },
            Err(e) => {
                self.show_error = true;
                self.error = Some(format!("Failed to send request: {e}"));
            }
        }
    }

    fn ui_schema_panel(&mut self, ctx: &egui::Context) {
        let download = self
            .schema_response
            .lock()
            .expect("failed to acquire lock")
            .take();
        if let Some(result) = download {
            match result {
                Ok(response) => match response.json() {
                    Ok(tables) => {
                        self.tables = Some(tables);
                    }
                    Err(_e) => {
                        self.schema_fetching_message = Some("Failed to parse schema".to_string());
                    }
                },
                Err(e) => {
                    self.schema_fetching_message = Some(format!("Failed to fetch schema: {e}"));
                }
            }
        }

        if self.tables.is_none() && self.schema_fetching_message.is_none() {
            self.http_fetch_schema();
        }

        egui::SidePanel::left("tables_panel")
            .default_width(100.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("schemas");
                ui.separator();

                egui::ScrollArea::vertical().show(ui, |ui| {
                    if let Some(tables) = &self.tables {
                        for table in tables.keys() {
                            ui.horizontal(|ui| {
                                let mut is_open = self.show_schema.contains(table);
                                ui.toggle_value(&mut is_open, table);
                                if is_open {
                                    if !self.show_schema.contains(table) {
                                        self.show_schema.insert(table.to_owned());
                                    }
                                } else {
                                    self.show_schema.remove(table);
                                }
                            });
                        }
                    } else if let Some(message) = &self.schema_fetching_message {
                        ui.label(message);
                    }
                });
            });

        if let Some(tables) = &self.tables {
            for (table, schema) in tables {
                if self.show_schema.contains(table) {
                    egui::Window::new(table)
                        .collapsible(true)
                        .resizable(true)
                        .show(ctx, |ui| {
                            egui_extras::TableBuilder::new(ui)
                                .striped(true)
                                .columns(
                                    egui_extras::Column::auto().at_least(100.0).resizable(true),
                                    2,
                                )
                                .header(20.0, |mut header| {
                                    for column in &["name", "nullable"] {
                                        header.col(|ui| {
                                            ui.strong(column.to_string());
                                        });
                                    }
                                })
                                .body(|mut body| {
                                    let fields = &schema.fields;
                                    for field in fields {
                                        body.row(18.0, |mut table_row| {
                                            table_row.col(|ui| {
                                                ui.label(&field.name);
                                            });
                                            table_row.col(|ui| {
                                                ui.label(field.nullable.to_string());
                                            });
                                        });
                                    }
                                });
                        });
                }
            }
        }
    }

    fn ui_query_panel(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            query_editor(ctx, ui, &mut self.query);
            ui.add_space(8.0);

            // Execute button
            if ui.button("Execute (Ctrl+Enter)").clicked()
                || (ui.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::Enter)))
            {
                self.http_execute_query();
            }

            let download = self
                .query_response
                .lock()
                .expect("failed to acquire lock")
                .take();
            if let Some(result) = download {
                self.show_query_executing = false;
                self.process_query_result(result);
            }
            ui.add_space(12.0);

            // Error display
            if let Some(err) = &self.error {
                egui::Window::new("Error")
                    .open(&mut self.show_error)
                    .collapsible(false)
                    .resizable(false)
                    .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
                    .show(ctx, |ui| {
                        ui.colored_label(egui::Color32::RED, err);
                    });
            }

            // Results display
            egui::Frame::default()
                .inner_margin(egui::vec2(8.0, 8.0))
                .show(ui, |ui| {
                    if self.show_query_executing {
                        ui.label("Executing query...");
                    } else {
                        display_query_results(ui, &self.results);
                    }
                });
        });
    }
}

fn query_editor(ctx: &egui::Context, ui: &mut egui::Ui, query: &mut String) {
    let theme = egui_extras::syntax_highlighting::CodeTheme::from_memory(ctx, ui.style());

    let mut layouter = |ui: &egui::Ui, string: &str, wrap_width: f32| {
        let language = "sql";
        let mut layout_job =
            egui_extras::syntax_highlighting::highlight(ctx, ui.style(), &theme, string, language);
        layout_job.wrap.max_width = wrap_width;
        ui.fonts(|f| f.layout_job(layout_job))
    };

    ui.add(
        egui::TextEdit::multiline(query)
            .code_editor()
            .desired_rows(5)
            .desired_width(f32::INFINITY)
            .layouter(&mut layouter)
            .hint_text("Enter SQL query (e.g. SELECT * FROM users)"),
    );
}

fn display_query_results(ui: &mut egui::Ui, results: &Option<QueryResult>) {
    egui::ScrollArea::both().show(ui, |ui| {
        if let Some(results) = results {
            egui_extras::TableBuilder::new(ui)
                .striped(true)
                .columns(
                    egui_extras::Column::auto().at_least(50.0).resizable(true),
                    results.columns.len(),
                )
                .header(20.0, |mut header| {
                    for column in &results.columns {
                        header.col(|ui| {
                            ui.strong(column);
                        });
                    }
                })
                .body(|mut body| {
                    for row in &results.rows {
                        body.row(18.0, |mut table_row| {
                            for value in row {
                                table_row.col(|ui| {
                                    ui.label(value);
                                });
                            }
                        });
                    }
                });
        }
    });
}

impl eframe::App for ROAPIUI {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Add menu bar
        egui::TopBottomPanel::top("menu_bar").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("ROAPI console");
                ui.add_space(ui.available_width() - 20.0); // Push menu to right

                ui.menu_button("⚙", |ui| {
                    if ui.button("Syntax Highlight Theme").clicked() {
                        self.show_settings = true;
                        ui.close_menu();
                    }
                });
            });

            ui.add_space(4.0);
        });

        // Settings window
        if self.show_settings {
            egui::Window::new("Settings")
                .open(&mut self.show_settings)
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::RIGHT_TOP, egui::vec2(-10.0, 10.0))
                .show(ctx, |ui| {
                    let mut theme =
                        egui_extras::syntax_highlighting::CodeTheme::from_memory(ctx, ui.style());
                    theme.ui(ui);
                    theme.clone().store_in_memory(ctx);
                });
        }

        self.ui_schema_panel(ctx);
        self.ui_query_panel(ctx);
    }
}
