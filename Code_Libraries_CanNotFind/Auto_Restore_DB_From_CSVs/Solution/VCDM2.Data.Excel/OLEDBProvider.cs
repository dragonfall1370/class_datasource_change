using System;
using System.Data;
using System.Data.OleDb;

namespace VCDM2.Data.Excel
{
    public class OLEDBProvider
    {
        string ConnectionString { get; set; }

        public OLEDBProvider(string filePath)
        {
            if (filePath.ToLower().EndsWith(".xls"))
            {
                // Connect EXCEL sheet with OLEDB using connection string
                // if the File extension is .XLS using below connection string
                //In following sample 'szFilePath' is the variable for filePath
                ConnectionString = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source = '{0}';Extended Properties=\"Excel 8.0;HDR=YES;\"", filePath);
            }
            else if (filePath.ToLower().EndsWith(".xlsx"))
            {
                // if the File extension is .XLSX using below connection string
                ConnectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source='{0}'; Extended Properties=\"Excel 12.0 Xml; HDR = YES\";", filePath);
            }
        }

        public DataTable Read(string sheetName = null, string tableName = null)
        {
            // Connect EXCEL sheet with OLEDB using connection string
            using (OleDbConnection conn = new OleDbConnection(ConnectionString))
            {
                try
                {
                    conn.Open();

					var sheetToRead = sheetName;

					if(string.IsNullOrWhiteSpace(sheetToRead))
					{
						var dtSchema = conn.GetOleDbSchemaTable(
						OleDbSchemaGuid.Tables, new object[] { null, null, null, "TABLE" });

						sheetToRead = dtSchema.Rows[0].Field<string>("TABLE_NAME");
					}
					else if(!sheetToRead.EndsWith("$"))
					{
						sheetToRead += "$";
					}

					OleDbDataAdapter objDA = new System.Data.OleDb.OleDbDataAdapter(
						string.Format("select * from [{0}]", sheetToRead), conn);

                    DataTable retVal =
						string.IsNullOrWhiteSpace(tableName) ? new DataTable() : new DataTable(tableName);

					objDA.Fill(retVal);

					return retVal;
                }
                catch(Exception e)
                {
                    return null;
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
        }

        public bool Write(string query)
        {
            //In above code '[Sheet1$]' is the first sheet name with '$' as default selector,
            // with the help of data adaptor we can load records in dataset		

            //write data in EXCEL sheet (Insert data)
            using (OleDbConnection conn = new OleDbConnection(ConnectionString))
            {
                try
                {
                    conn.Open();
                    OleDbCommand cmd = new OleDbCommand();
                    cmd.Connection = conn;
                    cmd.CommandText = query;
            //        @"Insert into [Sheet1$] (month,mango,apple,orange) 
            //VALUES ('DEC','40','60','80');";

                    var numRowAffect = cmd.ExecuteNonQuery();

                    return numRowAffect > 0;
                }
                catch (Exception ex)
                {
                    //exception here
                    return false;
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();
                }
            }            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query">UPDATE [Sheet1$] SET month = 'DEC' WHERE apple = 74;</param>
        /// <returns></returns>
        public bool Update(string query)
        {
            //update data in EXCEL sheet (update data)
            using (OleDbConnection conn = new OleDbConnection(ConnectionString))
            {
                try
                {
                    conn.Open();
                    OleDbCommand cmd = new OleDbCommand();
                    cmd.Connection = conn;
                    cmd.CommandText = query;
                    //UPDATE [Sheet1$] SET month = 'DEC' WHERE apple = 74;

                    var numRowAffect = cmd.ExecuteNonQuery();

                    return numRowAffect > 0;
                }
                catch (Exception ex)
                {
                    //exception here
                    return false;
                }
                finally
                {
                    conn.Close();
                    conn.Dispose();                    
                }
            }
        }
    }
}
