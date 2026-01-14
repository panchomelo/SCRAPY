# Scrapy - Modular Scraping Engine

Motor de scraping modular diseñado para alimentar pipelines de RAG, con integración vía webhooks HTTP para n8n.

## Características

- 🚀 **FastAPI Async**: Endpoints HTTP con respuesta inmediata + BackgroundTasks
- 🔄 **Retry Logic**: Tenacity para reintentos inteligentes en extracción y callbacks
- 📊 **Multi-Source**: Web (Playwright + BS4), PDF (pdfplumber), Excel (pandas), Social Media (Apify)
- 🗄️ **SQLite Persistence**: Trazabilidad completa de jobs con retención histórica
- 🔐 **API Key Auth**: Autenticación simple vía header `X-API-Key`
- 📝 **Structured Logging**: JSON logs con structlog para observabilidad
- 🎯 **Type-Safe**: Type hints estrictos + validación Pydantic
- 🛠️ **CLI Debugging**: Typer CLI para testing manual y análisis

## Estructura del Proyecto

```
scrapy/
├── api/              # FastAPI application
├── src/
│   ├── core/         # Engine y configuración
│   ├── database/     # Persistencia SQLite
│   ├── models/       # Pydantic schemas
│   ├── extractors/   # Web, PDF, Excel extractors
│   ├── services/     # Apify, callbacks
│   └── utils/        # Logging, exceptions
└── tests/            # Test suite
```

## Instalación

```bash
# 1. Clonar repositorio
git clone <repo-url>
cd scrapy

# 2. Activar direnv (automático si está instalado)
direnv allow

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Instalar Playwright browsers
playwright install

# 5. Configurar variables de entorno
cp .env.example .env
# Editar .env con tu API_KEY
```

## Uso

### Servidor API (para n8n)

```bash
python main.py serve --host 0.0.0.0 --port 8000
```

### CLI para debugging

```bash
# Extraer contenido web
python main.py scrape --source web --url "https://example.com" --debug

# Consultar estado de job
python main.py status <job_id>

# Ver estadísticas
python main.py stats
```

## Integración con n8n

```json
{
  "method": "POST",
  "url": "http://localhost:8000/api/v1/jobs/scrape",
  "headers": {
    "X-API-Key": "your-api-key",
    "Content-Type": "application/json"
  },
  "body": {
    "callback_url": "https://your-n8n-webhook.com/callback",
    "source": "web",
    "url": "https://example.com"
  }
}
```

## Desarrollo

```bash
# Ejecutar tests
pytest

# Type checking
mypy src/

# Formatear código
black src/ tests/
```

## Licencia

MIT
