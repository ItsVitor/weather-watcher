# 🌦️ Open-Meteo API - Referência de Integração

Este documento detalha os *endpoints*, parâmetros e variáveis da [API de Previsão de Tempo da Open-Meteo](https://open-meteo.com/en/docs) necessários para o funcionamento do sistema **Weather-Watcher**.

A Open-Meteo é uma API de código aberto, que não exige chaves de autenticação (API Keys) para uso não-comercial (limite de 10.000 requisições/dia) e agrega múltiplos modelos meteorológicos globais de alta resolução.

---

## 📍 Endpoint Base

Para previsões, todas as requisições HTTP do tipo `GET` devem ser direcionadas para:

```http
https://api.open-meteo.com/v1/forecast
```

---

## ⚙️ Parâmetros de Requisição (Query Parameters)

Para configurar a requisição, os parâmetros devem ser passados na URL.

### Parâmetros Obrigatórios

| Parâmetro | Tipo | Exemplo | Descrição |
| :--- | :--- | :--- | :--- |
| `latitude` | Float | `-20.31` | Coordenada geográfica (Eixo Y). |
| `longitude` | Float | `-40.31` | Coordenada geográfica (Eixo X). |

### Parâmetros Recomendados (Configuração)

| Parâmetro | Tipo | Exemplo | Descrição |
| :--- | :--- | :--- | :--- |
| `timezone` | String | `America/Sao_Paulo` | Ajusta o timestamp da resposta para o fuso horário local. Se não for definido, a API retornará o horário em UTC. |
| `forecast_days`| Integer | `1` ou `3` | Quantidade de dias de previsão. O padrão é 7, mas para otimizar o *payload* (tráfego de dados) do produtor... |

---

## 📊 Variáveis Meteorológicas (Mapeamento de Regras)

A API divide a entrega de dados em arrays de hora em hora (`hourly`) e agregações diárias (`daily`). Múltiplos valores podem ser solicitados separando-os por vírgula.

### 1. Variáveis Diárias (`daily=...`)

Utilizadas pelo `DailySummaryConsumer` para os alertas executados no início da manhã.

* `precipitation_probability_max`: Probabilidade máxima de chuva (%) ao longo do dia. *(Gatilho do Guarda-Chuva)*
* `uv_index_max`: O valor máximo do Índice UV para o dia. *(Gatilho do Protetor Solar)*
* `temperature_2m_max`: Temperatura máxima esperada a 2 metros do solo em °C. *(Gatilho de Conforto Térmico/Roupas Leves)*

### 2. Variáveis Horárias (`hourly=...`)

Utilizadas para o monitoramento contínuo em tempo quase real.

* `precipitation_probability`: Probabilidade de chuva (%) para cada hora específica. *(Gatilho do Alerta de Chuva Iminente para T+1)*
* `temperature_2m`: Temperatura exata do ar a 2 metros do solo em °C. *(Variável ingerida pelo `HeatwaveDetector` para alimentar o buffer da Onda de Calor)*

---

## 🚀 Exemplo Prático de Requisição

Para obter as variáveis que o `WeatherAPIProducer` precisará para a região metropolitana de Vitória (Lat: -20.31, Lon: -40.31), avaliando o dia atual e o próximo:

**Requisição HTTP GET:**

```http
https://api.open-meteo.com/v1/forecast?latitude=-20.31&longitude=-40.31&hourly=temperature_2m,precipitation_probability&daily=temperature_2m_max,uv_index_max,precipitation_probability_max&timezone=America%2FSao_Paulo&forecast_days=2
```

**Estrutura do JSON de Resposta:**
O objeto JSON retornado conterá dois grandes blocos (`hourly` e `daily`), onde os índices dos arrays de tempo (`time`) correspondem exatamente aos índices dos arrays de dados.

```json
{
  "latitude": -20.31,
  "longitude": -40.31,
  "timezone": "America/Sao_Paulo",
  "hourly": {
    "time": ["2026-05-06T00:00", "2026-05-06T01:00", "2026-05-06T02:00", "..."],
    "temperature_2m": [24.5, 24.2, 23.9, "..."],
    "precipitation_probability": [0, 10, 80, "..."]
  },
  "daily": {
    "time": ["2026-05-06", "2026-05-07"],
    "temperature_2m_max": [33.2, 36.1],
    "uv_index_max": [8.5, 9.2],
    "precipitation_probability_max": [80, 15]
  }
}
```
