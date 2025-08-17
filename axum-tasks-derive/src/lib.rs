use proc_macro::TokenStream;
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Fields, Lit, Meta, parse_macro_input};

/// Derive macro that automatically implements TaskHandler
///
/// Usage:
/// ```rust
/// #[derive(Task)]
/// #[task(description = "Analyzing telemetry data", retry = true)]
/// pub struct TelemetryAnalysis {
///     pub session_id: String,
///     pub file_path: String,
/// }
///
/// impl TelemetryAnalysis {
///     async fn execute(&self) -> TaskResult {
///         // Your implementation here
///     }
/// }
/// ```
#[proc_macro_derive(Task, attributes(task))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Parse task attributes
    let (description, retry) = match parse_task_attributes(&input.attrs) {
        Ok(attrs) => attrs,
        Err(e) => {
            panic!("Failed to parse task attributes: {}", e);
        }
    };

    let expanded = quote! {
        #[async_trait::async_trait]
        impl axum_tasks::TaskHandler for #name {
            async fn handle(&self) -> axum_tasks::TaskResult {
                self.execute().await
            }

            fn description(&self) -> String {
                #description.to_string()
            }

            fn is_retryable(&self, _error: &str) -> bool {
                #retry
            }
        }

        // Auto-register with task registry
        inventory::submit! {
            axum_tasks::TaskRegistration {
                name: stringify!(#name),
                handler: |task_data: &[u8]| {
                    let task_data = task_data.to_vec();
                    Box::pin(async move {
                        let task: #name = serde_json::from_slice(&task_data)
                            .map_err(|e| axum_tasks::TaskResult::PermanentError(
                                format!("Deserialization failed: {}", e)
                            ))?;
                        Ok(task.handle().await)
                    })
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derive macro that automatically implements HasTasks trait
///
/// Looks for a field named `tasks` or of type `AppTasks`
#[proc_macro_derive(HasTasks)]
pub fn derive_has_tasks(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let tasks_field = find_tasks_field(&input.data);

    let expanded = quote! {
        impl axum_tasks::HasTasks for #name {
            fn tasks(&self) -> &axum_tasks::AppTasks {
                &self.#tasks_field
            }

            fn tasks_mut(&mut self) -> &mut axum_tasks::AppTasks {
                &mut self.#tasks_field
            }
        }
    };

    TokenStream::from(expanded)
}

fn parse_task_attributes(attrs: &[Attribute]) -> Result<(String, bool), String> {
    let mut description = "Processing task".to_string();
    let mut retry = true;

    for attr in attrs {
        if attr.path().is_ident("task") {
            match &attr.meta {
                Meta::List(_meta_list) => {
                    attr.parse_nested_meta(|nested| {
                        if nested.path.is_ident("description") {
                            let value = nested.value()?;
                            if let Ok(lit_str) = value.parse::<Lit>() {
                                if let Lit::Str(s) = lit_str {
                                    description = s.value();
                                }
                            }
                        } else if nested.path.is_ident("retry") {
                            let value = nested.value()?;
                            if let Ok(lit_bool) = value.parse::<Lit>() {
                                if let Lit::Bool(b) = lit_bool {
                                    retry = b.value();
                                }
                            }
                        }
                        Ok(())
                    })
                    .map_err(|e| format!("Error parsing nested meta: {}", e))?;
                }
                Meta::Path(_) => {
                    // #[task] with no arguments - use defaults
                }
                Meta::NameValue(_) => {
                    return Err(
                        "task attribute should be a list: #[task(description = \"...\")]"
                            .to_string(),
                    );
                }
            }
        }
    }

    Ok((description, retry))
}

fn find_tasks_field(data: &Data) -> proc_macro2::TokenStream {
    match data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => {

                    // TODO: type checks
                    for field in &fields.named {
                        if let Some(ident) = &field.ident {
                            if ident == "tasks" {
                                return quote! { #ident };
                            }
                        }
                    }

                    for field in &fields.named {
                        if let Some(ident) = &field.ident {
                            let type_str = quote! { #(&field.ty) }.to_string();
                            if type_str.contains("AppTasks") {
                                return quote! { #ident };
                            }
                        }
                    }

                    panic!("No field named 'tasks' or of type 'AppTasks' found");
                }
                _ => panic!("HasTasks can only be derived for structs with named fields"),
            }
        }
        _ => panic!("HasTasks can only be derived for structs"),
    }
}
