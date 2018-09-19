using System;
using System.Data;

namespace VCDM.Data.Excel
{
    public class OLEDBProvider
    {
        string szConnectionString { get; set; }

        public OLEDBProvider(string szFilePath)
        {
            if (szFilePath.ToLower().EndsWith(""))
            {
                // Connect EXCEL sheet with OLEDB using connection string
                // if the File extension is .XLS using below connection string
                //In following sample 'szFilePath' is the variable for filePath
                szConnectionString = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source = '{0}';Extended Properties=\"Excel 8.0;HDR=YES;\"", szFilePath);
            }
            else
            {
                // if the File extension is .XLSX using below connection string
                szConnectionString = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source = '{0}';Extended Properties=\"Excel 12.0;HDR=YES;\"", szFilePath);
            }
        }
    }

    public Read()
    {
        System.Data.Common.
        // Connect EXCEL sheet with OLEDB using connection string
        using (OleDbConnection conn = new OleDbConnection(connectionString))
        {
            conn.Open();
            OleDbDataAdapter objDA = new System.Data.OleDb.OleDbDataAdapter
            ("select * from [Sheet1$]", conn);
            DataSet excelDataSet = new DataSet();
            objDA.Fill(excelDataSet);
            dataGridView1.DataSource = excelDataSet.Tables[0];
        }

        //In above code '[Sheet1$]' is the first sheet name with '$' as default selector,
        // with the help of data adaptor we can load records in dataset		

        //write data in EXCEL sheet (Insert data)
        using (OleDbConnection conn = new OleDbConnection(connectionString))
        {
            try
            {
                conn.Open();
                OleDbCommand cmd = new OleDbCommand();
                cmd.Connection = conn;
                cmd.CommandText = @"Insert into [Sheet1$] (month,mango,apple,orange) 
            VALUES ('DEC','40','60','80');";
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //exception here
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }

        //update data in EXCEL sheet (update data)
        using (OleDbConnection conn = new OleDbConnection(connectionString))
        {
            try
            {
                conn.Open();
                OleDbCommand cmd = new OleDbCommand();
                cmd.Connection = conn;
                cmd.CommandText = "UPDATE [Sheet1$] SET month = 'DEC' WHERE apple = 74;";
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                //exception here
            }
            finally
            {
                conn.Close();
                conn.Dispose();
            }
        }
    }
}
