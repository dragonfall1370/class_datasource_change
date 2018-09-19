using System;
using System.Data;
using VCDM2.Data.Excel;
using System.Data.SqlClient;
using VCDM2.Utility;
using System.IO;
using System.Collections.Generic;
using System.Configuration;

namespace VDDM2.CLI
{
    class Program
    {
		private const string csv2excel = @"/csv2excel";
		private const string excel2db = @"/excel2db";
		private const string csv2db = @"/csv2db";
		private const string bulkcsv2db = @"/bulkcsv2db";

		static void Main(string[] args)
        {
			var supportedCommands = new List<string>
			{
				csv2excel,
				excel2db,
				csv2db,
				bulkcsv2db
			};

			if(args == null || args.Length == 0)
			{
				PrintUsages();
				return;
			}

			var command = args[0];

			if (string.IsNullOrWhiteSpace(command) || !supportedCommands.Contains(command))
			{
				Console.WriteLine("Please enter a valid command name.");
				PrintUsages();

				return;
			}

			var commandParams = args.SubArrayDeepClone(1, args.Length - 1);

			switch(command)
			{
				case csv2excel:
					CSV2Excel(commandParams);
					break;
				case excel2db:
					Excel2DB(commandParams);
					break;
				case csv2db:
					CSV2DB(commandParams);
					break;
				case bulkcsv2db:
					BulkCSV2DB(commandParams);
					break;
				default: break;
			}
        }

		private static void PrintUsages()
		{
			Console.WriteLine(
							@"Usage:
========

	/csv2excel [base-path] [csv-file-name] [text-qualifier-character] [code-page:integer(ex: 65001 - Unicode (UTF-8), default: 1252 - ANSI Latin 1; Western European (Windows))] [delimiter-character] [end-of-line-characters]
	
	/excel2db [base-path] [excel-file-name]
	
	/csv2db [base-path] [csv-file-name] [text-qualifier-character] [code-page:integer(ex: 65001 - Unicode (UTF-8), default: 1252 - ANSI Latin 1; Western European (Windows))] [delimiter-character] [end-of-line-characters]
	
	/bulkcsv2db [base-path] [text-qualifier-character] [code-page:integer(ex: 65001 - Unicode (UTF-8), default: 1252 - ANSI Latin 1; Western European (Windows))] [delimiter-character] [end-of-line-characters]

=========");
		}

		private static void CSV2DB(string[] args)
		{
			var excelFilePath = CSV2Excel(args);

			if(string.IsNullOrWhiteSpace(excelFilePath))
			{
				Console.WriteLine("Could not convert csv to excel file.");
				return;
			}
			// import excel file to DB
			try
			{
				var fileInfo = new FileInfo(excelFilePath);
				if(fileInfo.Exists)
				{
					var tableName = fileInfo.Name.Replace(".xlsx", "");
					Excel2DB(new string[] { fileInfo.DirectoryName, tableName });
				}
			}
			catch(Exception ex)
			{
				Console.WriteLine(ex.Message);
				if (ex.InnerException != null)
				{
					Console.WriteLine(ex.InnerException.Message);
				}
				Console.WriteLine(ex.StackTrace);
			}
		}

		private static void BulkCSV2DB(string[] args)
		{
			if (args.Length == 0)
			{
				Console.WriteLine(@"Please enter the folder path that contains list of csv files to be imported");
				return;
			}

			var basePath = args[0]; //@"E:\Adam\Projects\OrcanIntelligence\DataExported\";

			if (string.IsNullOrWhiteSpace(basePath))
			{
				Console.WriteLine("Please enter the base path (folder path which contains excel file being imported.");
				return;
			}

			var textQualifier = args[1];

			if (textQualifier == null || textQualifier.Length > 1)
			{
				Console.WriteLine("Please enter a valid text qualifier character.");
				return;
			}

			var codePage = args[2];
			int codePageInt = 0;

			if (string.IsNullOrWhiteSpace(codePage) || !int.TryParse(codePage, out codePageInt))
			{
				Console.WriteLine("Please enter a valid code page which is a integer.");
				return;
			}

			string delimiter = ",";
			string eol = @"\r\n";

			if (args.Length == 5)
			{
				delimiter = args[3];

				if (string.IsNullOrWhiteSpace(delimiter) || delimiter.Length != 1)
				{
					Console.WriteLine("Please enter a valid delimiter character.");
					return;
				}

				eol = args[4];

				if (string.IsNullOrWhiteSpace(eol) || (eol != @"\r\n" && eol != @"\n"))
				{
					Console.WriteLine(@"Please enter a valid end of line characters. That should be either \r\n or \n");
					return;
				}
			}

			try
			{
				var dir = new DirectoryInfo(basePath);
				var fileList = dir.GetFiles("*.csv", SearchOption.TopDirectoryOnly);
				if(fileList != null && fileList.Length > 0)
				{
					foreach (var f in fileList)
					{
						var ags = new string[] { basePath, f.Name, textQualifier, codePage, delimiter, eol };
						CSV2DB(ags);
					}
				}
			}
			catch(Exception ex)
			{
				Console.WriteLine(ex.Message);
				if (ex.InnerException != null)
				{
					Console.WriteLine(ex.InnerException.Message);
				}
				Console.WriteLine(ex.StackTrace);
			}
		}

		private static string CSV2Excel(string[] args)
		{
			if (args.Length == 0)
			{
				Console.WriteLine(@"Please enter command's parameters.\nEx: E:\Adam\Projects\OrcanIntelligence\DataExported\ Note.csv");
				return null;
			}

			var basePath = args[0]; //@"E:\Adam\Projects\OrcanIntelligence\DataExported\";

			if (string.IsNullOrWhiteSpace(basePath))
			{
				Console.WriteLine("Please enter the base path (folder path which contains excel file being imported.");
				return null;
			}

			if (!basePath.EndsWith(@"\"))
			{
				basePath += @"\";
			}

			var sourceFile = args[1];

			if (string.IsNullOrWhiteSpace(sourceFile))
			{
				Console.WriteLine("Please enter the csv file that you want to convert to excel file.");
				return null;
			}

			var filePath = string.Format("{0}{1}", basePath, sourceFile);

			if (!File.Exists(filePath))
			{
				Console.WriteLine("Could not find the given source file. Please check the file path again.");
				return null;
			}

			var textQualifier = args[2];

			if (textQualifier == null || textQualifier.Length > 1)
			{
				Console.WriteLine("Please enter a valid text qualifier character.");
				return null;
			}

			var codePage = args[3];
			int codePageInt = 0;

			if (string.IsNullOrWhiteSpace(codePage) || !int.TryParse(codePage, out codePageInt))
			{
				Console.WriteLine("Please enter a valid code page which is a integer.");
				return null;
			}

			string delimiter = ",";
			string eol = @"\r\n";

			if(args.Length == 6)
			{
				delimiter = args[4];

				if (string.IsNullOrWhiteSpace(delimiter) || delimiter.Length != 1)
				{
					Console.WriteLine("Please enter a valid delimiter character.");
					return null;
				}

				eol = args[5];

				if (string.IsNullOrWhiteSpace(eol) || (eol != @"\r\n" && eol != @"\n"))
				{
					Console.WriteLine("Please enter a valid end of line characters.");
					return null;
				}
			}

			char? finalTextQualifier = null;
			if(textQualifier.Length == 1)
			{
				finalTextQualifier = textQualifier[0];
			}

			var convertedExcelFilePath = EPPlusProvider.CSV2XLSX(
				basePath
				, sourceFile
				, finalTextQualifier
				, codePageInt
				, delimiter[0]
				, eol);

			return convertedExcelFilePath;
		}

		private static void Excel2DB(string[] args)
		{
			if (args.Length == 0)
			{
				Console.WriteLine(@"Please enter base path and then the table name.\nEx: E:\Adam\Projects\OrcanIntelligence\DataExported\ Note");
				return;
			}

			var basePath = args[0]; //@"E:\Adam\Projects\OrcanIntelligence\DataExported\";

			if (string.IsNullOrWhiteSpace(basePath))
			{
				Console.WriteLine("Please enter the base path (folder path which contains excel file being imported.");
				return;
			}

			if (!basePath.EndsWith(@"\"))
			{
				basePath += @"\";
			}

			var excelFileName = args[1];

			if (string.IsNullOrWhiteSpace(excelFileName))
			{
				Console.WriteLine("Please enter the table name (also is the name of excel sheet which contains the data being imported) that you want to create in destination database.");
				return;
			}

			var filePath = string.Format("{0}{1}.xlsx", basePath, excelFileName);

			if (!File.Exists(filePath))
			{
				Console.WriteLine("Could not find the given excel file. Please check the file path again.");
				return;
			}

			var oleProvider = new OLEDBProvider(filePath);

			Console.WriteLine("Starting to read into datatable");

			// wrap the table name, so that it support special name
			var tableName = string.Format("[dbo].[{0}]", excelFileName);

			var dataTable = oleProvider.Read(null, tableName);

			if (dataTable != null)
			{
				Console.WriteLine("File have been read into datatable");

				dataTable.TableName = dataTable.TableName ?? tableName;

				var importResult = Import(dataTable);

				if (importResult)
				{
					Console.WriteLine("Data imported successfully");
				}
				else
				{
					Console.WriteLine("Data imported unsuccessfully");
				}
			}
			else
			{
				Console.WriteLine("File have not been read into datatable. Please try again");
			}
		}

		private static bool Import(DataTable myDataTable)
        {
			//string connectionString = @"Data Source=dmpfra.vinceredev.com;
			//        Network Library=DBMSSOCN;Initial Catalog=OrcanIntelligencePro;
			//        User ID=sa;Password=123$%^qwe;
			//        Connection Timeout=1800";

			//string connectionString = @"Data Source=localhost\MSSQLSRV2017DEV;
			//                 Network Library=DBMSSOCN;Initial Catalog=VC_DM;
			//                 User ID=sa;Password=Olala3334;
			//                 Connection Timeout=1800";

			var connectionString =
				ConfigurationManager.ConnectionStrings["VCDM"].ConnectionString;


			//@"Data Source = dmpfra.vinceredev.com/MSSQLSERVER; Integrated Security=true; Initial Catalog=YourDatabase";
			try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
					// open a connection
					connection.Open();
					
					// delete table if exists
					string deleteTableQuery =
						string.Format(
							@"IF OBJECT_ID('{0}', 'U') IS NOT NULL
								DROP TABLE {1};", myDataTable.TableName, myDataTable.TableName);

					SqlCommand command = new SqlCommand(deleteTableQuery, connection);

					Console.WriteLine(string.Format("Deleting table '{0}' if exists", myDataTable.TableName));

					command.ExecuteNonQuery();
					
					// create table
					// create table if not exists 
					string createTableQuery =
                        string.Format(@"Create Table {0}
                            (", myDataTable.TableName);
                    //( SaleDate datetime, ItemName nvarchar(1000),ItemsCount int)
                    foreach (DataColumn c in myDataTable.Columns)
                    {
						var columnNameWrapped = string.Format("[{0}]", c.ColumnName);

						var dataType = c.DataType.ToSQLServerDatabaseEngineType().ToString().ToLower();

                        createTableQuery += columnNameWrapped + " " + dataType;
                        //if(c.AllowDBNull)
                        //{
                        //    createTableQuery += " NULL";
                        //}

                        if(dataType == "nvarchar")
                        {
                            createTableQuery += "(max)";
                        }

                        createTableQuery += " null, ";
                    }

                    createTableQuery = createTableQuery.Substring(0, createTableQuery.Length - 2);

                    createTableQuery +=
                        @"
                            )";

					command = new SqlCommand(createTableQuery, connection);

					Console.WriteLine(string.Format("Creating table '{0}'", myDataTable.TableName));

                    command.ExecuteNonQuery();

                    // do bulk insert
                    using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
                    {
                        foreach (DataColumn c in myDataTable.Columns)
                            bulkCopy.ColumnMappings.Add(c.ColumnName, c.ColumnName);

                        bulkCopy.DestinationTableName = myDataTable.TableName;

                        Console.WriteLine(string.Format("Start bulk inserting data into table '{0}'", myDataTable.TableName));

						// set timeout
						bulkCopy.BulkCopyTimeout = 18000;

						bulkCopy.WriteToServer(myDataTable);

                        Console.WriteLine(string.Format("Finished bulk inserting data into table '{0}'", myDataTable.TableName));

                        return true;
                    }
                }

                //using (var conn = new SqlConnection(connectionString))
                //{
                //    var bulkCopy = new SqlBulkCopy(conn);
                //    bulkCopy.DestinationTableName = myDataTable.TableName;
                //    conn.Open();
                //    var schema = conn.GetSchema("Columns", new[] { null, null, table, null });
                //    foreach (DataColumn sourceColumn in dt.Columns)
                //    {
                //        foreach (DataRow row in schema.Rows)
                //        {
                //            if (string.Equals(sourceColumn.ColumnName, (string)row["COLUMN_NAME"], StringComparison.OrdinalIgnoreCase))
                //            {
                //                bulkCopy.ColumnMappings.Add(sourceColumn.ColumnName, (string)row["COLUMN_NAME"]);
                //                break;
                //            }
                //        }
                //    }
                //    bulkCopy.WriteToServer(dt);

                //    return true;
                //}
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }
    }
}
