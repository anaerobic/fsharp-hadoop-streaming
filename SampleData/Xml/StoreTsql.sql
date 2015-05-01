USE AdventureWorks2008R2
GO

SELECT TOP(42)
	[BusinessEntityID]	AS 'BusinessEntityID',
    [Name]				AS 'Name',
    [SalesPersonID]		AS 'SalesPersonID',
    [Demographics]		AS 'Demographics',
    [ModifiedDate]		AS 'Modified'
  FROM [AdventureWorks2008R2].[Sales].[Store]
  FOR XML PATH ('Store'), ROOT ('Root');
GO