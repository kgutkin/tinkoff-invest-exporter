CREATE TABLE IF NOT EXISTS transactions(
    id INTEGER PRIMARY KEY,
    errandNumber INTEGER,
    conclusionDate VARCHAR(20),
    time VARCHAR(20),
    tradingPlatform VARCHAR(50),
    tradeRegime VARCHAR(20),
    type VARCHAR(30),
    assetShortName VARCHAR(255),
    ticker VARCHAR(10),
    price REAL,
    priceCurrency VARCHAR(8),
    count integer,
    sumWithoutNkd real,
    nkd real,
    sum real,
    transactionCurrency varchar(8),
    commission real,
    commissionCurrency varchar(8),
    repoRate real,
    counterparty varchar(20),
    settlementDay varchar(20),
    deliveryDate varchar(20),
    brokerStatus varchar(20),
    contractType varchar(255),
    contractNumber integer,
    contractDate varchar(20)
);

DROP TABLE IF EXISTS transaction_headers;

CREATE TABLE transaction_headers(
    columnIndex integer primary key,
    name varchar(40)
);

create table if not exists settings(
    name varchar(50) primary key,
    value varchar(255)
);