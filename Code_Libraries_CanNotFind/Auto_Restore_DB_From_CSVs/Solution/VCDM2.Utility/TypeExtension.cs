using Microsoft.SqlServer.Server;
using System;
using System.Data;

namespace VCDM2.Utility
{
    public static class TypeExtension
    {
        //public static SqlDbType ToSqlDbType(this Type clrType)
        //{
        //    var s = new SqlMetaData("", SqlDbType.NVarChar, clrType);
        //    return s.SqlDbType;
        //}


        public static Type ToClrType(SqlDbType sqlType)
        {
            switch (sqlType)
            {
                case SqlDbType.BigInt:
                    return typeof(long?);

                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:
                case SqlDbType.VarBinary:
                    return typeof(byte[]);

                case SqlDbType.Bit:
                    return typeof(bool?);

                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                case SqlDbType.Xml:
                    return typeof(string);

                case SqlDbType.DateTime:
                case SqlDbType.SmallDateTime:
                case SqlDbType.Date:
                case SqlDbType.Time:
                case SqlDbType.DateTime2:
                    return typeof(DateTime?);

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return typeof(decimal?);

                case SqlDbType.Float:
                    return typeof(double?);

                case SqlDbType.Int:
                    return typeof(int?);

                case SqlDbType.Real:
                    return typeof(float?);

                case SqlDbType.UniqueIdentifier:
                    return typeof(Guid?);

                case SqlDbType.SmallInt:
                    return typeof(short?);

                case SqlDbType.TinyInt:
                    return typeof(byte?);

                case SqlDbType.Variant:
                case SqlDbType.Udt:
                    return typeof(object);

                case SqlDbType.Structured:
                    return typeof(DataTable);

                case SqlDbType.DateTimeOffset:
                    return typeof(DateTimeOffset?);

                default:
                    throw new ArgumentOutOfRangeException("sqlType");
            }
        }

        public static SqlDbType ToSqlType(this Type clrType)
        {
            if(clrType == typeof(long?))
                return SqlDbType.BigInt;

            if (clrType == typeof(bool?) || clrType == typeof(bool))
                return SqlDbType.Bit;

            if (clrType == typeof(string))
                return SqlDbType.NVarChar;

            if (clrType == typeof(DateTime?) || clrType == typeof(DateTime))
                return SqlDbType.DateTime;

            if (clrType == typeof(decimal?) || clrType == typeof(decimal))
                return SqlDbType.Decimal;

            if (clrType == typeof(double?) || clrType == typeof(double))
                return SqlDbType.Float;

            if (clrType == typeof(int?) || clrType == typeof(int))
                return SqlDbType.Int;

            if (clrType == typeof(float?) || clrType == typeof(float))
                return SqlDbType.Real;

            if (clrType == typeof(Guid?) || clrType == typeof(Guid))
                return SqlDbType.UniqueIdentifier;

            if (clrType == typeof(short?) || clrType == typeof(short))
                return SqlDbType.SmallInt;

            if (clrType == typeof(byte?) || clrType == typeof(byte))
                return SqlDbType.TinyInt;

            if (clrType == typeof(object))
                return SqlDbType.Variant;

            if (clrType == typeof(DataTable))
                return SqlDbType.Structured;

            if (clrType == typeof(DateTimeOffset?) || clrType == typeof(DateTimeOffset))
                return SqlDbType.DateTimeOffset;

            throw new ArgumentOutOfRangeException("clrType");
        }

        public static string ToSQLServerDatabaseEngineType(this Type clrType)
        {
            if (clrType == typeof(Int64)) return "bigint";
            //if (clrType == typeof(Byte[])) return "binary";
            if (clrType == typeof(Boolean)) return "bit";
			//if (clrType == typeof(String)) return "char";
			//if (clrType == typeof(Char[])) return "char";
			//if (clrType == typeof(DateTime)) return "date 1";
			if (clrType == typeof(DateTime)) return "datetime";
			//if (clrType == typeof(DateTime)) return "datetime2";
            if (clrType == typeof(DateTimeOffset)) return "datetimeoffset";
            if (clrType == typeof(Decimal)) return "decimal";
            //if (clrType == typeof(Byte[])) return "FILESTREAM (varbinary(max))";
            if (clrType == typeof(Double)) return "float";
            //if (clrType == typeof(Byte[])) return "image";
            if (clrType == typeof(Int32)) return "int";
            if (clrType == typeof(Decimal)) return "money";
            //if (clrType == typeof(String)) return "nchar";
            //if (clrType == typeof(Char[])) return "nchar";
            //if (clrType == typeof(String)) return "ntext";
            //if (clrType == typeof(Char[])) return "ntext";
            //if (clrType == typeof(Decimal)) return "numeric";
            if (clrType == typeof(String)) return "nvarchar";
            if (clrType == typeof(Char[])) return "nvarchar";
            if (clrType == typeof(Single)) return "real";
            //if (clrType == typeof(Byte[])) return "rowversion";
            //if (clrType == typeof(DateTime)) return "smalldatetime";
            if (clrType == typeof(Int16)) return "smallint";
            //if (clrType == typeof(Decimal)) return "smallmoney";
            if (clrType == typeof(Object)) return "sql_variant";
            //if (clrType == typeof(String)) return "text";
            //if (clrType == typeof(Char[])) return "text";
            if (clrType == typeof(TimeSpan)) return "time";
            //if (clrType == typeof(Byte[])) return "timestamp";
            if (clrType == typeof(Byte)) return "tinyint";
            if (clrType == typeof(Guid)) return "uniqueidentifier";
            if (clrType == typeof(Byte[])) return "varbinary";
            //if (clrType == typeof(String)) return "varchar";
            //if (clrType == typeof(Char[])) return "varchar";
            //if (clrType == typeof(System.Xml)) return "xml";

            throw new ArgumentOutOfRangeException("clrType");
        }
    }
}
