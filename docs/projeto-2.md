# Projeto 2

Projeto 1 (weather-watcher) será utilizado como base e melhorado na forma do Projeto 2.

Principal diferença: na etapa 2, deveremos obrigatoriamente usar a API Kafka Streams (documentação oficial "Streams Developer Guide" na íntegra em /home/vitor-ubuntu/weather-watcher/docs/dsl-api.md.

## Funcionalidades esperadas

1. O "evento composto" da etapa 1 *HeatwaveDetector* deve passar a ser implementado utilizando Kafka Streams;
2. Definir aplicações/topologias Kafka Streams que permitam a detecção de outras 3 situações de interesse no sistema;
    1. Álgebra de Allen deve ser utilizada na detecção de uma dessas situações;

Obs: Tanto operações Stateless quanto Stateful da DSL do Streams devem ser utilizadas: filtros, maps, joins, agregações, janelas temporais e/ou de eventos.
