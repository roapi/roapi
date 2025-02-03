use roapi_ui::app::ROAPIUI;

#[cfg(not(target_arch = "wasm32"))]
pub fn run_query_ui() -> eframe::Result {
    //env_logger::init(); // Log to stderr (if you run with `RUST_LOG=debug`).

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([400.0, 300.0])
            .with_min_inner_size([300.0, 220.0]),
        //.with_icon(
        //    // NOTE: Adding an icon is optional
        //    eframe::icon_data::from_png_bytes(&include_bytes!("../assets/icon-256.png")[..])
        //        .expect("Failed to load icon"),
        //),
        ..Default::default()
    };
    eframe::run_native(
        "ROAPI console",
        native_options,
        Box::new(|_cc| Ok(Box::new(ROAPIUI::new()))),
    )
}

//#[wasm_bindgen]
#[cfg(target_arch = "wasm32")]
pub fn run_query_ui() -> eframe::Result {
    use eframe::wasm_bindgen::JsCast as _;

    // Redirect `log` message to `console.log` and friends:
    eframe::WebLogger::init(log::LevelFilter::Debug).ok();

    let web_options = eframe::WebOptions::default();

    wasm_bindgen_futures::spawn_local(async {
        let document = web_sys::window()
            .expect("No window")
            .document()
            .expect("No document");

        let canvas = document
            .get_element_by_id("the_canvas_id")
            .expect("Failed to find the_canvas_id")
            .dyn_into::<web_sys::HtmlCanvasElement>()
            .expect("the_canvas_id was not a HtmlCanvasElement");

        let start_result = eframe::WebRunner::new()
            .start(
                canvas,
                web_options,
                Box::new(|_cc| Ok(Box::new(ROAPIUI::new()))),
            )
            .await;

        // Remove the loading text and spinner:
        if let Some(loading_text) = document.get_element_by_id("loading_text") {
            match start_result {
                Ok(_) => {
                    loading_text.remove();
                }
                Err(e) => {
                    loading_text.set_inner_html(
                        "<p> The app has crashed. See the developer console for details. </p>",
                    );
                    panic!("Failed to start eframe: {e:?}");
                }
            }
        }
    });

    Ok(())
}

fn main() {
    run_query_ui().expect("failed to start UI");
}
