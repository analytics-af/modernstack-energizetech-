Projeto EnergizeTech - Data Engineering Pipeline

1. Introdução
Este projeto foi desenvolvido para a EnergizeTech, uma empresa fictícia especializada na venda de baterias para carros elétricos. A necessidade de uma infraestrutura de dados robusta surge da expansão do mercado de veículos elétricos, onde a análise de vendas, previsão de demanda e identificação de oportunidades de mercado são essenciais para o sucesso. Este pipeline de engenharia de dados fornece uma solução completa, automatizando o fluxo de dados de vendas da captura até a transformação e armazenamento no Data Warehouse (DW).

2. Contexto da Empresa
A EnergizeTech atua no mercado crescente de veículos elétricos, fornecendo baterias de alta performance para fabricantes de veículos e consumidores finais, que adquirem diretamente nas lojas online e físicas. A empresa precisa monitorar o desempenho regional de vendas, comportamento de clientes e tendências de mercado para impulsionar a tomada de decisões estratégicas.

3. Objetivos do Projeto
O objetivo deste projeto é construir um Data Warehouse que centralize os dados comerciais da EnergizeTech, permitindo análises detalhadas sobre:

- Desempenho de vendas
- Análise regional
- Comportamento dos clientes
- Tendências de mercado
- A solução inclui a automação de um pipeline de ETL (Extract, Transform, Load) que utiliza ferramentas modernas para capturar, transformar e carregar dados comerciais da empresa.

4. Arquitetura de Solução
   
- Tecnologias Utilizadas
- MongoDB (NoSQL): Armazena dados flexíveis de vendas, clientes e produtos.
- Airflow (Astro): Orquestra o pipeline de ETL, controlando o fluxo de captura e carga de dados de forma automatizada.
- Docker Compose: Gerencia o ambiente de execução dos containers, incluindo MongoDB, Airflow, DBT e PostgreSQL.
- DBT (Data Build Tool): Aplica as regras de negócio e transforma os dados em fatos e dimensões.
- PostgreSQL: Armazena os dados transformados no Data Warehouse, disponibilizando-os para análise.
  

![image](https://github.com/user-attachments/assets/0a521a92-9876-4e91-8797-bacce5ce4c9a)


5. Descrição do Fluxo de Dados
    1. Captura de Dados com Airflow
    Os dados são capturados do MongoDB e carregados no PostgreSQL no schema public. As tabelas maiores são carregadas de forma incremental, enquanto tabelas pequenas utilizam o método de replace.
    O pipeline é orquestrado com o Airflow, que automatiza e gerencia a integridade do processo de ETL.
    2. Transformação com DBT
    O DBT transforma os dados brutos em fatos e dimensões.
    As tabelas de fatos agregam as vendas por período, região e produto. As dimensões detalham as características de clientes e produtos.
    O processo de transformação inclui a aplicação de SCD Tipo 2 para manter o histórico de dados.
    3. Armazenamento e Consulta no Data Warehouse
    Os dados transformados são armazenados no PostgreSQL e utilizados para consultas analíticas e relatórios por meio de ferramentas de Business Intelligence (BI), permitindo a análise de desempenho de vendas e identificação de oportunidades.

6. Pipeline de Engenharia de Dados
O pipeline desenvolvido faz o seguinte:

    - Trunca as tabelas no schema public do PostgreSQL antes de realizar novas cargas.
    - Limpeza de dados de fatos: O pipeline remove dados de vendas dos últimos 7 dias para permitir a recarga.
    - Transferência de dados incrementais quando a data do MongoDB for maior do que a data máxima da fato: Carga incremental para tabelas grandes, como sales e transactions.
    - Carga completa para tabelas pequenas: Tabelas menores, como customers e products, utilizam o método de replace.
    - Transformações com DBT: As tabelas staging são criadas como views temporárias e as tabelas finais são materializadas como fatos e dimensões.
      
7. Instalação e Execução
Para rodar o projeto, siga os passos abaixo:

    Setup no EC2 (AWS)
    Conectar ao EC2: Acesse sua instância via SSH.
    
    bash
    Copiar código
    ssh -i "seu_arquivo.pem" ubuntu@ec2-endereco
    Instalar o Docker:
    
    bash
    Copiar código
    sudo apt update
    sudo apt install docker.io -y
    sudo systemctl start docker
    sudo systemctl enable docker
    sudo usermod -aG docker ${USER}
    Instalar o Docker Compose:
    
    bash
    Copiar código
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    Instalar Astro Airflow:
    
    bash
    Copiar código
    curl -sSL https://install.astronomer.io | sudo bash
    Configurar o Ambiente:
    
    Clone o repositório: git clone https://github.com/seuusuario/energizetech.git
    Navegue até a pasta do projeto: cd energizetech
    Suba o ambiente com Docker Compose: docker-compose up -d
    Acessar o Airflow:
    O Airflow estará acessível em http://localhost:8080.
   
8. Estrutura do Repositório
- /dags: Pipelines do Airflow
- /dbt: Modelos de transformação no DBT
- /docker-compose.yml: Configuração dos containers
- /scripts: Scripts auxiliares e consultas SQL
  
9. Casos de Uso e Benefícios
- Melhor entendimento do comportamento dos clientes.
- Otimização da distribuição de estoques nas regiões de maior demanda.

Contato:
- andrel.faria1@gmail.com 
- https://www.linkedin.com/in/andrelfaria
