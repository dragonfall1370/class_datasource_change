using OfficeOpenXml;
using OfficeOpenXml.Table;
using System;
using System.IO;
using System.Text;

namespace VCDM2.Data.Excel
{
	public class EPPlusProvider
	{
		// 65001	utf-8	Unicode (UTF-8)
		// 1252	windows-1252	ANSI Latin 1; Western European (Windows)
		// 1250	windows-1250	ANSI Central European; Central European (Windows)
		// 28591	iso-8859-1	ISO 8859-1 Latin 1; Western European (ISO)
		// 28592	iso-8859-2	ISO 8859-2 Central European; Central European(ISO)
		public static string CSV2XLSX(
			string basePath,
			string csvFileName,
			char? textQualifier = null,
			int encodingCodePage = 1252,
			char delimiter = ',',
			string eol = @"\r\n",
			bool firstRowIsHeader = true)
		{
			if (string.IsNullOrWhiteSpace(basePath) || string.IsNullOrWhiteSpace(csvFileName))
			{
				return null;
			}

			try
			{
				if (basePath.EndsWith(@"\"))
				{
					basePath = basePath.TrimEnd('\\');
				}

				string csvFilePath = string.Format(@"{0}\{1}", basePath, csvFileName);

				var xlsxFileName = csvFileName.Replace(".csv", ".xlsx");

				string outputPath = string.Format(@"{0}\Output", basePath);

				string xlsxFilePath = string.Format(@"{0}\{1}", outputPath, xlsxFileName);

				string worksheetsName = csvFileName.Substring(0, csvFileName.Length - 4);

				var format = new ExcelTextFormat();
				format.Encoding = Encoding.GetEncoding(encodingCodePage);
				format.Delimiter = delimiter;

				if (eol.StartsWith(@"\n"))
				{
					format.EOL = "\n";
				}

				if (textQualifier.HasValue)
				{
					format.TextQualifier = textQualifier ?? textQualifier.Value;
				}

				// ty to create output path
				Directory.CreateDirectory(outputPath);
				Console.WriteLine(string.Format("Prepared output path at: {0}", outputPath));
				// delete output file if exists
				if (File.Exists(xlsxFilePath))
				{
					File.Delete(xlsxFilePath);
					Console.WriteLine(string.Format("Confirmed destination file {0} not exist.", xlsxFilePath));
				}

				using (ExcelPackage package = new ExcelPackage(new FileInfo(xlsxFilePath)))
				{
					ExcelWorksheet worksheet = package.Workbook.Worksheets.Add(worksheetsName);

					Console.WriteLine(string.Format("Start loading source file {0}.", csvFilePath));

					worksheet.Cells["A1"].LoadFromText(new FileInfo(csvFilePath), format, TableStyles.None, firstRowIsHeader);

					Console.WriteLine(string.Format("Start saving destination file {0}.", xlsxFilePath));

					package.Save();
				}

				Console.WriteLine(string.Format("Finished converting csv file to xlsx file at: {0}", xlsxFilePath));

				return xlsxFilePath;
			}
			catch(Exception ex)
			{
				Console.WriteLine(ex.Message);
				if(ex.InnerException != null)
				{
					Console.WriteLine(ex.InnerException.Message);
				}
				Console.WriteLine(ex.StackTrace);

				//Console.ReadKey();

				return null;
			}
		}
	}
}
