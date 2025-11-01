#[cfg(feature = "http")]
mod http_impl {
    use crate::db::Aurora;
    use crate::network::http_models::{QueryPayload, document_to_json, json_to_insert_data};
    use crate::types::FieldType;
    use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
    use serde::Deserialize;
    use serde_json::Value as JsonValue;
    use std::sync::Arc;

    #[derive(Deserialize)]
    struct CollectionPayload {
        name: String,
        fields: Vec<(String, FieldType, bool)>,
    }

    async fn get_all_docs(db: web::Data<Arc<Aurora>>, path: web::Path<String>) -> impl Responder {
        let collection_name = path.into_inner();
        match db.get_all_collection(&collection_name).await {
            Ok(docs) => {
                let json_docs: Vec<JsonValue> = docs.iter().map(document_to_json).collect();
                HttpResponse::Ok().json(json_docs)
            }
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        }
    }

    async fn create_collection(
        db: web::Data<Arc<Aurora>>,
        payload: web::Json<CollectionPayload>,
    ) -> impl Responder {
        let fields: Vec<(String, FieldType, bool)> = payload
            .fields
            .iter()
            .map(|(name, ft, unique)| (name.clone(), ft.clone(), *unique))
            .collect();

        match db.new_collection(&payload.name, fields) {
            Ok(_) => HttpResponse::Created().finish(),
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        }
    }

    async fn insert_document(
        db: web::Data<Arc<Aurora>>,
        path: web::Path<String>,
        data: web::Json<JsonValue>,
    ) -> impl Responder {
        let collection_name = path.into_inner();
        let doc_to_insert = data.into_inner();

        match json_to_insert_data(doc_to_insert) {
            Ok(insert_data) => match db.insert_map(&collection_name, insert_data) {
                Ok(id) => match db.get_document(&collection_name, &id) {
                    Ok(Some(doc)) => HttpResponse::Created().json(document_to_json(&doc)),
                    Ok(None) => {
                        HttpResponse::NotFound().body("Failed to retrieve document after creation")
                    }
                    Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
                },
                Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
            },
            Err(e) => HttpResponse::BadRequest().body(e.to_string()),
        }
    }

    async fn delete_document(
        db: web::Data<Arc<Aurora>>,
        path: web::Path<(String, String)>,
    ) -> impl Responder {
        let (collection_name, doc_id) = path.into_inner();
        let key = format!("{}:{}", collection_name, doc_id);
        match db.delete(&key).await {
            Ok(_) => HttpResponse::NoContent().finish(),
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        }
    }

    async fn query_collection(
        db: web::Data<Arc<Aurora>>,
        path: web::Path<String>,
        payload: web::Json<QueryPayload>,
    ) -> impl Responder {
        let collection_name = path.into_inner();
        match db.execute_dynamic_query(&collection_name, &payload).await {
            Ok(docs) => {
                let json_docs: Vec<JsonValue> = docs.iter().map(document_to_json).collect();
                HttpResponse::Ok().json(json_docs)
            }
            Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
        }
    }

    async fn not_found(req: HttpRequest) -> impl Responder {
        let (path, method) = (req.path().to_string(), req.method().to_string());
        let msg = format!("404 Not Found: No route for {} {}", method, path);
        println!("{}", msg);
        HttpResponse::NotFound().body(msg)
    }

    pub async fn run_http_server(db: Arc<Aurora>, addr: &str) -> std::io::Result<()> {
        let db_data = web::Data::new(db);
        println!("🚀 Starting HTTP server at http://{}", addr);

        HttpServer::new(move || {
            App::new()
                .app_data(db_data.clone())
                .route("/collections", web::post().to(create_collection))
                .service(
                    web::scope("/collections")
                        .route("/{name}", web::get().to(get_all_docs))
                        .route("/{name}/docs", web::post().to(insert_document))
                        .route("/{name}/docs/{id}", web::delete().to(delete_document))
                        .route("/{name}/query", web::post().to(query_collection)),
                )
                .default_service(web::to(not_found))
        })
        .bind(addr)?
        .run()
        .await
    }
}

#[cfg(feature = "http")]
pub use http_impl::run_http_server;
