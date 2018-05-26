/*
 * Copyright (c) 2018 Bryan Bullard (bbullard at gmail)
 * All Rights Reserved. No warranties
 *
 * 2/18/2018
 *
 * The following DDL, DML demonstrate the use of Kimball-style Slowly Changing Dimensions (SCD)
 * Type 1 and Type 2;
 *
 * Handles early-arriving facts/late-arriving dimension scenario (not all dimension attributes 
 * are known at fact row arrival) by temporarily stubbing with "unknown" member. 
 * 
 * Target MS SQL Server 2012
 */

/* DDL Starts Here */

--USE [<database>];

SET ANSI_NULLS ON;
GO

IF OBJECT_ID('dbo.StageProcessChangeSet', 'P') IS NOT NULL
  DROP PROCEDURE dbo.StageProcessChangeSet;
IF OBJECT_ID('dbo.StageChangeAudit', 'U') IS NOT NULL
  DROP TABLE dbo.StageChangeAudit;
IF OBJECT_ID('dbo.StageChangeSet', 'U') IS NOT NULL
  DROP TABLE dbo.StageChangeSet;
IF OBJECT_ID('dbo.FactOrder', 'U') IS NOT NULL
  DROP TABLE dbo.FactOrder;
IF OBJECT_ID('dbo.DimCustomer', 'U') IS NOT NULL
  DROP TABLE dbo.DimCustomer;
IF OBJECT_ID('dbo.DimDate', 'U') IS NOT NULL
  DROP TABLE dbo.DimDate;
IF OBJECT_ID('dbo.DimProduct', 'U') IS NOT NULL
  DROP TABLE dbo.DimProduct;
IF OBJECT_ID('dbo.UnknownKey', 'FN') IS NOT NULL
  DROP FUNCTION dbo.UnknownKey;
GO

/*
 * Unknown Key Value used throughout
 */
CREATE FUNCTION dbo.UnknownKey() RETURNS INT
AS BEGIN RETURN 0; END;
GO

/*
 * Customer Dimension
 *
 * SCD Type 2 changes allowed for customer change of address
 * by detecting a different address line or postal code for
 * the same customer ID. 
 */
CREATE TABLE dbo.DimCustomer (
  [Key] int IDENTITY(1,1) NOT NULL,
  CustomerID int NULL,
  FullName varchar(64) NULL,
  AddressLine1 varchar(32) NULL,
  City varchar(32) NULL,
  PostalCode varchar(16) NULL,
  StateProvince varchar(32) NULL,
  CONSTRAINT PK_DimCustomer PRIMARY KEY CLUSTERED ([Key]),
  CONSTRAINT UK_DimCustomer_ScdKey UNIQUE NONCLUSTERED (
    CustomerID ASC,
    AddressLine1,
    PostalCode
    )
  );
GO

/*
 * Date Dimension 
 * 
 * Only SCD Type 2 changes allowed
 */
CREATE TABLE dbo.DimDate (
  [Key] int IDENTITY(1,1) NOT NULL,
  [Date] date NULL,
  [Month] tinyint NULL,
  [MonthName] varchar(32) NULL,
  [Quarter] tinyint NULL,
  [Year] smallint NULL,
  CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED ([Key]),
  CONSTRAINT UK_DimDate_ScdKey UNIQUE NONCLUSTERED ([Date] ASC)
  );
GO

/*
 * Product Dimension
 * 
 * SCD Type 2 changes allowed for product price change by 
 * detecting a different price for the same product ID.
 */
CREATE TABLE dbo.DimProduct (
  [Key] int IDENTITY(1,1) NOT NULL,
  ProductID int NULL,
  [Description] varchar(64) NULL,
  Price money NULL,
  CONSTRAINT PK_DimProduct PRIMARY KEY CLUSTERED ([Key]),
  CONSTRAINT UK_DimProduct_ScdKey UNIQUE NONCLUSTERED (
    ProductID ASC,
    Price
    )
  );
GO

/*
 * Order Fact
 *
 * OrderNumber is granularity attribute
 */
CREATE TABLE dbo.FactOrder (
  CustomerKey int NOT NULL DEFAULT dbo.UnknownKey(),
  DateKey int NOT NULL DEFAULT dbo.UnknownKey(),
  DateTimeUtc smalldatetime NULL,
  ProductKey int NOT NULL DEFAULT dbo.UnknownKey(),
  OrderNumber int NOT NULL,
  Amount money NOT NULL,
  PartitionInfo binary(2) NULL,
  CONSTRAINT PK_FactOrder PRIMARY KEY CLUSTERED (OrderNumber ASC),
  CONSTRAINT FK_FactOrder_CustomerKey FOREIGN KEY(CustomerKey) 
    REFERENCES dbo.DimCustomer ([Key]),
  CONSTRAINT FK_FactOrder_DateKey FOREIGN KEY(DateKey) 
    REFERENCES dbo.DimDate ([Key]),
  CONSTRAINT FK_FactOrder_ProductKey FOREIGN KEY(ProductKey) 
    REFERENCES dbo.DimProduct ([Key])
  );
GO

ALTER TABLE dbo.FactOrder 
  CHECK CONSTRAINT FK_FactOrder_CustomerKey, 
    FK_FactOrder_DateKey, FK_FactOrder_ProductKey;

GO

/*
 * Change Set
 *
 * 1NF structure with same grain as target fact or bridge table.
 * Temporarily holds arriving data for processing.
 * Normally this type of object would exist outside
 * the data warehouse query area.
 */
CREATE TABLE dbo.StageChangeSet (
  CustomerID int NULL,
  CustomerFullName varchar(64) NULL,
  CustomerAddressLine1 varchar(32) NULL,
  CustomerCity varchar(32) NULL,
  CustomerPostalCode varchar(16) NULL,
  CustomerStateProvince varchar(32) NULL,
  OrderNumber INT NOT NULL,
  OrderDateTimeUtc smalldatetime NULL,
  OrderAmount money NULL,
  ProductID int NULL,
  ProductDescription varchar(64) NULL,
  ProductPrice money NULL,
  CONSTRAINT PK_StageChangeSet PRIMARY KEY (OrderNUmber ASC)
);
GO

/*
 * Change Audit
 *
 * Allows for the auditing of changes processed.
 * Normally this type of object would exist outside
 * the data warehouse query area.
 */
CREATE TABLE dbo.StageChangeAudit (
  [TimeStamp] rowversion NOT NULL,
  [Time] datetime NOT NULL DEFAULT GETUTCDATE(),
  [Type] varchar(32) NULL,
  [Table] sysname NULL,
  [Key] INT NULL,
  RowData XML,
  CONSTRAINT PK_StageCHangeAudit PRIMARY KEY CLUSTERED ([TimeStamp])
  );
GO

/*
 * Process Change Set
 *
 * Procedure to process change set data.
 * Normally this type of object would exist outside
 * the data warehouse query area.
 */
CREATE PROCEDURE dbo.StageProcessChangeSet
AS
BEGIN

SET NOCOUNT ON;

/*
 * Table changes and audit logging handled within a single transction
 * with the MERGE statement.
 */

BEGIN TRY

MERGE
INTO 
  dbo.DimCustomer TARGET
USING (
  SELECT
    CustomerID,
    MIN(CustomerFullName) AS FullName,
    CustomerAddressLine1 AS AddressLine1,
    MIN(CustomerCity) AS City,
    CustomerPostalCode AS PostalCode,
    MIN(CustomerStateProvince) AS StateProvince
  FROM
    dbo.StageChangeSet
  WHERE
    CustomerID IS NOT NULL
  GROUP BY
    CustomerID,
    CustomerAddressLine1,
    CustomerPostalCode
  ) SOURCE
  ON SOURCE.CustomerID = TARGET.CustomerID /* NULL Customer ID will default to the Unknown Key */
    AND (SOURCE.AddressLine1 = TARGET.AddressLine1 OR (SOURCE.AddressLine1 IS NULL AND TARGET.AddressLine1 IS NULL))
    AND (SOURCE.PostalCode = TARGET.PostalCode OR (SOURCE.PostalCode IS NULL AND TARGET.PostalCode IS NULL))
WHEN MATCHED AND (
  TARGET.FullName != SOURCE.FullName 
  OR (TARGET.FullName IS NULL AND SOURCE.FullName IS NOT NULL)
  OR (TARGET.FullName IS NOT NULL AND SOURCE.FullName IS NULL)
  ) OR (
  TARGET.City != SOURCE.City 
  OR (TARGET.City IS NULL AND SOURCE.City IS NOT NULL)
  OR (TARGET.City IS NOT NULL AND SOURCE.City IS NULL)
  ) OR (
  TARGET.City != SOURCE.City 
  OR (TARGET.City IS NULL AND SOURCE.City IS NOT NULL)
  OR (TARGET.City IS NOT NULL AND SOURCE.City IS NULL)
  ) THEN /* Perform a Type 1 changes */
  UPDATE 
  SET TARGET.FullName = SOURCE.FullName,
    TARGET.City = SOURCE.City,
    TARGET.StateProvince = SOURCE.StateProvince
WHEN NOT MATCHED THEN /* Perform a Type 2 change */
  INSERT (
    CustomerID,
    FullName,
    AddressLine1,
    City,
    PostalCode,
    StateProvince
  ) VALUES (
    SOURCE.CustomerID,
    SOURCE.FullName,
    SOURCE.AddressLine1,
    SOURCE.City,
    SOURCE.PostalCode,
    SOURCE.StateProvince
    )
OUTPUT
  'DimCustomer',
  CASE $ACTION 
    WHEN 'UPDATE' THEN 'Type 1 (Overwrite)'
    WHEN 'INSERT' THEN 'Type 2 (Add Row)'
    ELSE NULL
  END,
  INSERTED.[Key],
  TRY_CONVERT(XML,'<CustomerID>'+CAST(INSERTED.CustomerID AS VARCHAR)+'</CustomerID>')
INTO
  dbo.StageChangeAudit (
    [Table],
    [Type],
    [Key],
    RowData
    )
;

MERGE
INTO 
  dbo.DimDate TARGET
USING (
  SELECT
    CAST(OrderDateTimeUtc AS DATE) AS [Date],
    DATEPART(MONTH,CAST(OrderDateTimeUtc AS DATE)) AS [Month],
    DATENAME(MONTH,CAST(OrderDateTimeUtc AS DATE)) AS [MonthName],
    DATEPART(QUARTER,CAST(OrderDateTimeUtc AS DATE)) AS [Quarter],
    DATEPART(YEAR,CAST(OrderDateTimeUtc AS DATE)) AS [Year]
  FROM
    dbo.StageChangeSet
  WHERE
    CAST(OrderDateTimeUtc AS DATE) IS NOT NULL
  GROUP BY
    CAST(OrderDateTimeUtc AS DATE)
  ) SOURCE
  ON SOURCE.[Date] = TARGET.[Date]
WHEN NOT MATCHED THEN /* Only accepts Type 2 changes */
  INSERT (
    [Date],
    [Month],
    [MonthName],
    [Quarter],
    [Year]
  ) VALUES (
    SOURCE.[Date],
    SOURCE.[Month],
    SOURCE.[MonthName],
    SOURCE.[Quarter],
    SOURCE.[Year]
    )
OUTPUT
  'DimDate',
  CASE $ACTION 
    WHEN 'INSERT' THEN 'Type 2 (Add Row)'
    ELSE NULL
  END,
  INSERTED.[Key],
  TRY_CONVERT(XML,'<Date>'+CONVERT(VARCHAR,INSERTED.[Date],120)+'</Date>')
INTO
  dbo.StageChangeAudit (
    [Table],
    [Type],
    [Key],
    RowData
    )
;

MERGE
INTO 
  dbo.DimProduct TARGET
USING (
  SELECT
    ProductID,
    MIN(ProductDescription) AS [Description],
    ProductPrice AS Price
  FROM
    dbo.StageChangeSet
  WHERE
    ProductID IS NOT NULL
  GROUP BY
    ProductID,
    ProductPrice
  ) SOURCE
  ON SOURCE.ProductID = TARGET.ProductID /* NULL Product ID will default to the Unknown Key */
    AND (SOURCE.Price = TARGET.Price OR (SOURCE.Price IS NULL AND TARGET.Price IS NULL))
WHEN MATCHED AND (
  TARGET.[Description] != SOURCE.[Description] 
  OR (TARGET.[Description] IS NULL AND SOURCE.[Description] IS NOT NULL)
  OR (TARGET.[Description] IS NOT NULL AND SOURCE.[Description] IS NULL)
  ) THEN /* Perform a Type 1 changes */
  UPDATE 
  SET TARGET.[Description] = SOURCE.[Description]
WHEN NOT MATCHED THEN /* Perform a Type 2 change */
  INSERT (
    ProductID,
    [Description],
    Price
  ) VALUES (
    SOURCE.ProductID,
    [Description],
    Price
    )
OUTPUT
  'DimProduct',
  CASE $ACTION 
    WHEN 'UPDATE' THEN 'Type 1 (Overwrite)'
    WHEN 'INSERT' THEN 'Type 2 (Add Row)'
    ELSE NULL
  END,
  INSERTED.[Key],
  TRY_CONVERT(XML,'<ProductID>'+CAST(INSERTED.ProductID AS VARCHAR)+'</ProductID>')
INTO
  dbo.StageChangeAudit (
    [Table],
    [Type],
    [Key],
    RowData
    )
;

MERGE
INTO 
  dbo.FactOrder TARGET
USING (
  SELECT
    ISNULL(Customer.[Key],dbo.UnknownKey()) AS CustomerKey,
    ISNULL([Date].[Key],dbo.UnknownKey()) AS DateKey,
    ChangeSet.OrderDateTimeUtc AS DateTimeUtc,
    ISNULL(Product.[Key],dbo.UnknownKey()) AS ProductKey,
    ChangeSet.OrderNumber,
    ISNULL(ChangeSet.OrderAmount,0) AS Amount
  FROM
    dbo.StageChangeSet ChangeSet
    LEFT OUTER JOIN
    dbo.DimCustomer Customer
      ON ChangeSet.CustomerID = Customer.CustomerID
      AND (
        ChangeSet.CustomerAddressLine1 = Customer.AddressLine1
        OR (ChangeSet.CustomerAddressLine1 IS NULL AND Customer.AddressLine1 IS NULL)
      AND (
        ChangeSet.CustomerPostalCode = Customer.PostalCode
        OR (ChangeSet.CustomerPostalCode IS NULL AND Customer.PostalCode IS NULL)
        )
      )
    LEFT OUTER JOIN
    dbo.DimDate [Date]
      ON CAST(ChangeSet.OrderDateTimeUtc AS DATE) = [Date].[Date]
    LEFT OUTER JOIN
    dbo.DimProduct Product
      ON ChangeSet.ProductID = Product.ProductID
      AND (
        ChangeSet.ProductPrice = Product.Price
        OR (ChangeSet.ProductPrice IS NULL AND Product.Price IS NULL)
        )
  ) SOURCE
  ON SOURCE.OrderNumber = TARGET.OrderNumber
WHEN MATCHED AND (
  TARGET.CustomerKey != SOURCE.CustomerKey
  OR (
    TARGET.DateTimeUtc != SOURCE.DateTimeUtc 
    OR (TARGET.DateTimeUtc IS NULL AND SOURCE.DateTimeUtc IS NOT NULL)
    OR (TARGET.DateTimeUtc IS NOT NULL AND SOURCE.DateTimeUtc IS NULL)
    )
  OR TARGET.DateKey != SOURCE.DateKey
  OR TARGET.ProductKey != SOURCE.ProductKey
  OR TARGET.Amount != SOURCE.Amount
  ) THEN
  UPDATE 
    SET TARGET.CustomerKey = SOURCE.CustomerKey,
      TARGET.DateKey = SOURCE.DateKey,
      TARGET.DateTimeUtc = SOURCE.DateTimeUtc,
      TARGET.ProductKey = SOURCE.ProductKey,
      TARGET.Amount = SOURCE.Amount
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    CustomerKey,
    DateKey,
    DateTimeUtc,
    OrderNumber,
    ProductKey,
    Amount
  ) VALUES (
    SOURCE.CustomerKey,
    SOURCE.DateKey,
    SOURCE.DateTimeUtc,
    SOURCE.OrderNumber,
    SOURCE.ProductKey,
    SOURCE.Amount
    )
OUTPUT
  'FactOrder',
  CASE $ACTION 
    WHEN 'UPDATE' THEN 'Type 1 (Overwrite)'
    WHEN 'INSERT' THEN 'Type 2 (Add Row)'
    ELSE NULL
  END,
  NULL,
  TRY_CONVERT(XML,'<OrderNumber>'+CAST(INSERTED.OrderNumber AS VARCHAR)+'</OrderNumber>')
INTO
  dbo.StageChangeAudit (
    [Table],
    [Type],
    [Key],
    RowData
    )
;

/* Automatically Clear the change set table once 
 * all changes are applied.
 * However, if there is a failure, preserve the 
 * contents for re-try.
 */
TRUNCATE TABLE dbo.StageChangeSet;

END TRY
BEGIN CATCH

THROW;
RETURN 1;

END CATCH

END
GO


/* DML Starts Here */

SET NOCOUNT ON;

/* Initialize dimension tables with Unknown Key */
SET IDENTITY_INSERT dbo.DimCustomer ON;
INSERT dbo.DimCustomer ([Key]) VALUES (dbo.UnknownKey());
SET IDENTITY_INSERT dbo.DimCustomer OFF;

SET IDENTITY_INSERT dbo.DimDate ON;
INSERT dbo.DimDate ([Key]) VALUES (dbo.UnknownKey());
SET IDENTITY_INSERT dbo.DimDate OFF;

SET IDENTITY_INSERT dbo.DimProduct ON;
INSERT dbo.DimProduct ([Key]) VALUES (dbo.UnknownKey());
SET IDENTITY_INSERT dbo.DimProduct OFF;


/* Two orders from new customers on 01-01-2013 */

INSERT
  dbo.StageChangeSet VALUES (
    100001, /* Customer ID */
    'Aldous Huxley', /* Customer Full Name */
    '1100 Congress Ave', /* Customer Address Line 1 */
    'Austin', /* Customer City */
    '78701', /* Customer Postal Code */
    'TX', /* Customer State */
    000001, /* Order Number */
    '01-01-2013 01:02:03', /* Order Date */
    10.77, /* Order Amount */
    300001, /* Product ID */
    'Brave New World', /* Product Description */
    9.95 /* Product Price */
    );
GO

INSERT
  dbo.StageChangeSet VALUES (
    100002, /* Customer ID */
    'Ernest Hemingway', /* Customer Full Name */
    '1100 Congress Ave', /* Customer Address Line 1 */
    'Austin', /* Customer City */
    '78701', /* Customer Postal Code */
    'TX', /* Customer State */
    000002, /* Order Number */
    '01-01-2013 02:04:05', /* Order Date */
    12.94, /* Order Amount */
    300002, /* Product ID */
    'Old Man and the Sea', /* Product Description */
    11.95 /* Product Price */
    );
GO

EXEC dbo.StageProcessChangeSet;
GO

/* Customer changes mind on 01-02-2013 and selects different product.  
 * Slightly more expensive.
 */

INSERT
  dbo.StageChangeSet VALUES (
    100001, /* Customer ID */
    'Aldous Huxley', /* Customer Full Name */
    '1100 Congress Ave', /* Customer Address Line 1 */
    'Austin', /* Customer City */
    '78701', /* Customer Postal Code */
    'TX', /* Customer State */
    000001, /* Order Number */
    '01-02-2013 01:02:03', /* Order Date */
    16.18, /* Order Amount */
    300010, /* Product ID */
    'Brave New World - Revisited', /* Product Description */
    14.95 /* Product Price */
    );

EXEC dbo.StageProcessChangeSet;
GO


/* Customer re-orders same proudct on 01-01-2014 but this time there is a 
 * change of address and the price has gone up 
 */
INSERT
  dbo.StageChangeSet VALUES (
    100002, /* Customer ID */
    'Ernest Hemingway', /* Customer Full Name */
    '1600 Pennsylvania Ave', /* Customer Address Line 1 */
    'Washington', /* Customer City */
    '20500', /* Customer Postal Code */
    'DC', /* Customer State */
    000003, /* Order Number */
    '01-01-2014 01:02:03', /* Order Date */
    21.60, /* Order Amount */
    300002, /* Product ID */
    'Old Man and the Sea', /* Product Description */
    19.95 /* Product Price */
    );
GO

EXEC dbo.StageProcessChangeSet;
GO

/* Customer changes name; On 02-01-2014 orders proudct based on a prior review
 * However, business wants all the customer's records with same address
 * to reflect this name change. 
 */
INSERT
  dbo.StageChangeSet VALUES (
    100002, /* Customer ID */
    'Gertrud Stein', /* Customer Full Name */
    '1600 Pennsylvania Ave', /* Customer Address Line 1 */
    'Washington', /* Customer City */
    '20500', /* Customer Postal Code */
    'DC', /* Customer State */
    000004, /* Order Number */
    '02-01-2014 01:02:03', /* Order Date */
    16.18, /* Order Amount */
    300010, /* Product ID */
    'Brave New World - Revisited', /* Product Description */
    14.95 /* Product Price */
    );
GO

/* Also, clerk manually enters order but has incomeplete information. */
INSERT
  dbo.StageChangeSet VALUES (
    NULL, /* Customer ID */
    NULL, /* Customer Full Name */
    NULL, /* Customer Address Line 1 */
    NULL, /* Customer City */
    NULL, /* Customer Postal Code */
    NULL, /* Customer State */
    000010, /* Order Number */
    '02-01-2014 12:02:03', /* Order Date */
    16.18, /* Order Amount */
    400010, /* Product ID */
    NULL, /* Product Description */
    14.95 /* Product Price */
    );

EXEC dbo.StageProcessChangeSet;
GO

/* Incomepete order information is processed. */
INSERT
  dbo.StageChangeSet VALUES (
    200001, /* Customer ID */
    'Thomas S Eliot', /* Customer Full Name */
    '1100 Congress Ave', /* Customer Address Line 1 */
    'Austin', /* Customer City */
    '78701', /* Customer Postal Code */
    'TX', /* Customer State */
    000010, /* Order Number */
    '02-01-2014 12:02:03', /* Order Date */
    16.18, /* Order Amount */
    400010, /* Product ID */
    'The Second-Order Mind', /* Product Description */
    14.95 /* Product Price */
    );

EXEC dbo.StageProcessChangeSet;
GO


/* Star query, returning all */
SELECT
  [Date].[Year],
  [Date].[MonthName],
  FactOrder.DateTimeUtc,
  FactOrder.OrderNumber,
  FactOrder.Amount AS OrderAmount,
  Customer.FullName,
  Customer.AddressLine1,
  Customer.City,
  Customer.StateProvince,
  Customer.PostalCode,
  Product.ProductID,
  Product.[Description],
  Product.Price AS ProductPrice
  
FROM
  FactOrder
  JOIN
  DimCustomer Customer
    ON FactOrder.CustomerKey = Customer.[Key]
  JOIN
  DimDate [Date]
    ON FactOrder.DateKey = [Date].[Key]
  JOIN
  DimProduct Product
    ON FactOrder.ProductKey = Product.[Key]
ORDER BY
  DateTimeUtc
    ;

SELECT
  *
FROM 
  StageChangeAudit
ORDER BY
  [Table] ASC,
  [Time] ASC;

/* End */