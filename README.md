# Project Description
Представляет из себя сборник дагов, которые забирают сырые данные о деятельности интернет-магазина по доставке Готовой еды из трех разных источников и складируют их в DWH на Postgres.

Финальный даг собирает две витрины:
- `dm_settlement_report` - Содержит аггрегированные данные по заказам и выплаченным клиентам бонусам
- `dm_courier_ledger` - Данные по курьерской доставке. Рейтинг курьера, чаевые, кол-во заказов и сумма, которую интернет-магазин должен выплатить за отчетные период

# Sources
Источники данных:
- Бонусная система - Postgres
- Система заказов - MongoDB
- Система курьерской службы - API 

# DWH Layers
DWH имеет несколько основных слоев:
- `stg` - Стейдж слой, хранит данные из источников AS IS
- `dds` - Хранит перелитые из стейджа данные в таблицах в 3НФ. Имеет классическую модель данных "Созвездие"
- `cdm` - Хранит аггрегированные витрины для бизнес-пользователей

# Project Structure
### DAGs
В папке `dags` находятся основные даги:
- `dags/init-dwh-dag.py` - Даг инициализирующий все слои хранилища. Запускается один раз, после запуска docker-compose с Airflow. Выполняет DDL скприпт `sql/init-dwh-ddl.sql` и триггерит на запуск даг `load-stg-dwh-dag.py`

![init-dwh-dag](https://github.com/Leonidee/three-source-dwh/tree/master/addons/init-dwh-dag.png?raw=true)

- `dags/load-stg-dwh-dag.py`  - Загрузка или апдейт STG слоя

![load-stg-dwh-dag](https://github.com/Leonidee/three-source-dwh/tree/master/addons/load-stg-dwh-dag.png?raw=true)

- `dags/load-dds-dwh-dag.py` - Загрузка DDS слоя

![load-dds-dwh-dag](https://github.com/Leonidee/three-source-dwh/tree/master/addons/load-dds-dwh-dag.png?raw=true)

- `dags/load-cdm-dag.py` - Финальный даг, обновляющий витрины в CDM слое

![load-cdm-dag](https://github.com/Leonidee/three-source-dwh/tree/master/addons/load-cdm-dag.png?raw=true)

### Bussiness Logic
Вся бизнес логика разбита по пакетам и находится в папке `pkg`:

- `pkg/main.py` - Здесь объявлены основные классы и соответствующие методы, передвигающие данные
- `pkg/objs.py` - Объявление классов данных
- `pkg/tasks.py` - Таски, используемые в дагах
- `pkg/utils.py` - Различные утилиты. Класс конекторов к источникам, вспомагательные ETL-классы и тому подобные
- `pkg/errors.py` - Объявление классов исключений, которые могут быть вызваны внутри бизнес-логики

### SQL
DDL скрипты собраны в единый файл `sql/init-dwh-ddl.sql`, который инициализирует все слови хранилища одной транзакцией.
