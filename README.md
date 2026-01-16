# Scrapy - Modular Scraping Engine

Modular scraping engine designed to feed RAG pipelines

## Features

- 🚀 **FastAPI Async**: HTTP endpoints with immediate responses + BackgroundTasks
- 🔄 **Retry Logic**: Tenacity for smart retries in extraction and callbacks
- 📊 **Multi-Source**: Web (Playwright + BS4), PDF (pdfplumber), Excel (pandas), Social Media (Apify)
- 🗄️ **SQLite Persistence**: Full job traceability with historical retention
- 🔐 **API Key Auth**: Simple authentication via the `X-API-Key` header
- 📝 **Structured Logging**: JSON logs with structlog for observability
- 🎯 **Type-Safe**: Strict type hints + Pydantic validation
- 🛠️ **CLI Debugging**: Typer CLI for manual testing and inspection