# Kafka Plugin — Requisitos

## Objetivo

Plugin do OpenIDEA para gerenciar e inspecionar recursos Kafka durante o desenvolvimento, permitindo conectar a clusters, explorar tópicos e acompanhar mensagens.

## Requisitos

### 1. Gerenciamento de conexões/servidores
- Cadastrar e gerenciar conexões com clusters Kafka.
- Configurar dados de conexão:
  - Nome da conexão
  - Bootstrap servers (host/IP e porta)
  - Propriedades adicionais de cliente, quando necessário
- Suportar autenticação e segurança:
  - Sem autenticação
  - SASL/PLAIN, SASL/SCRAM (quando aplicável)
  - SSL/TLS (certificados, truststore/keystore conforme necessário)
- Testar conexão antes de salvar.
- Permitir múltiplas conexões/perfis (ambientes locais, dev, staging etc.).

### 2. Tópicos
- Listar tópicos do cluster.
- Visualizar detalhes do tópico (configurações básicas, número de partições, fator de replicação, etc.).
- Criar tópico (nome, partições, fator de replicação, configurações opcionais).
- Excluir tópico, com confirmação.
- **Consumir mensagens**:
  - Ler mensagens em tempo real e/ou a partir de offset/início/fim.
  - Exibir chave, valor, timestamp, headers, partição e offset.
  - Filtrar/buscar mensagens por conteúdo, chave, header ou intervalo.
- **Publicar mensagens**:
  - Enviar mensagem para um tópico.
  - Definir chave, valor, headers e partição de destino (opcional).

### 3. Partições
- Listar partições de um tópico.
- Exibir informações por partição:
  - Líder
  - Réplicas e ISR (in-sync replicas)
  - Offsets iniciais/finais
  - Lag/posicionamento quando aplicável
- Permitir navegação/consumo por partição específica.

### 4. Nós/Brokers e cluster
- Listar brokers/nós do cluster.
- Exibir informações básicas do cluster e dos brokers (ID, host, porta, status/disponibilidade).
- Mostrar controlador/metadata relevante quando disponível.

### 5. Consumer groups (desejável)
- Listar consumer groups.
- Visualizar lag por grupo/tópico/partição.
- Inspecionar offsets consumidos e offsets finais.

### 6. Experiência de uso
- Integrar com a UI do OpenIDEA (tool window/painéis apropriados).
- Tratar erros de conexão/autenticação com mensagens claras.
- Não expor segredos em logs; armazenar credenciais de forma segura quando aplicável.
- Permitir atualizar/recarregar dados do cluster manualmente.
