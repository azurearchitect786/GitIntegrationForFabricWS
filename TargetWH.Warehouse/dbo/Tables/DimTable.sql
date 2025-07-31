CREATE TABLE [dbo].[DimTable] (

	[SalesRepID] int NULL, 
	[RepSourceID] int NULL, 
	[FirstName] varchar(20) NULL, 
	[LastName] varchar(20) NULL, 
	[Region] varchar(20) NULL, 
	[StartDate] date NULL, 
	[EndDate] date NULL, 
	[IsCurrent] bit NULL, 
	[Hash] varchar(100) NULL
);