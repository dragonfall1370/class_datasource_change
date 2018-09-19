using Microsoft.Office.Interop.Excel;
using Microsoft.Office.Tools.Excel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VCDM2.Data.Excel
{
	public class MsOfficeInteropExcelProvider
	{
		public void abc(string basePath, string csvFileName)
		{
			if(!basePath.EndsWith("\\"))
			{
				basePath += "\\";
			}

			var csvFilePath = basePath + csvFileName;

			var xlsxFileName = csvFileName.Replace(".csv", ".xlsx");

			var xlsxFilePath = basePath + xlsxFileName;

			if (File.Exists(csvFilePath))
			{
				File.Delete(csvFilePath);
			}
			////Save as xlsx using Spire
			//Workbook workbook = new Workbook();
			//workbook.LoadFromFile(Util.CSVFileName, ",", 1, 1);
			//Worksheet sheet = workbook.Worksheets[0];
			//sheet.Name = "csv to excel";
			//workbook.SaveToFile(Util.XLSXFileName, ExcelVersion.Version2010);

			////Delete Evaluation Warning using EPPlus
			//var workbookFileInfo = new FileInfo(Util.XLSXFileName);
			//using (var excelPackage = new ExcelPackage(workbookFileInfo))
			//{
			//	var worksheet = excelPackage.Workbook.Worksheets.SingleOrDefault(x => x.Name == "Evaluation Warning");
			//	excelPackage.Workbook.Worksheets.Delete(worksheet);
			//	excelPackage.Save();
			//}
		}
	}
}
