using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

namespace VisualogyxETL
{
    class Program
    {

        static void Main(string[] args)
        {
            OriginDatabaseContext origindatabasecontext = new OriginDatabaseContext();
            ReplicaDatabaseContext replicadatabasecontext = new ReplicaDatabaseContext();

            var services = new ServiceCollection();
            services.AddDbContext<OriginDatabaseContext>()
                    .AddDbContext<ReplicaDatabaseContext>();

            //Console.WriteLine("Hello World!");
            string[] TablesList = { "UserInfo", "Department", "Job", "Progress" };
            string ErrorId = "", duplicateIdError = "";

            foreach (string tbname in TablesList)
            {
                SqlParameter[] ParameterName;
                duplicateIdError += tbname + "(";
                ErrorId += tbname + "(";
                string query = "select * from " + tbname;

                var ReplicaDataReader = replicadatabasecontext.Database.GetService<IRawSqlCommandBuilder>().Build(query).ExecuteReader(replicadatabasecontext.Database.GetService<IRelationalConnection>()).DbDataReader;
                var OriginalDataReader = origindatabasecontext.Database.GetService<IRawSqlCommandBuilder>().Build(query).ExecuteReader(origindatabasecontext.Database.GetService<IRelationalConnection>()).DbDataReader;

                var ColumnNamesWithDataTypeList = ReplicaDataReader.GetColumnSchema().Select(o => new { ColumnName = o.ColumnName, DataTypeName = o.DataTypeName }).ToList();
                IEnumerable<string> ColumnNamesList = OriginalDataReader.GetColumnSchema().Select(o => o.ColumnName);

                ParameterName = new SqlParameter[5];
                replicadatabasecontext.Database.CloseConnection();

                while (OriginalDataReader.Read())
                {
                    int i = 0;
                    string columnnames = "";

                    foreach (var columnname in ColumnNamesList)
                    {
                        if (ColumnNamesWithDataTypeList[i].DataTypeName == "uniqueidentifier" && ColumnNamesWithDataTypeList[i].ColumnName == columnname)
                        {
                            byte[] bytes = new byte[16];
                            BitConverter.GetBytes(Convert.ToInt32(OriginalDataReader[columnname])).CopyTo(bytes, 0);
                            Guid GuidObj = new Guid(bytes);
                            ParameterName.SetValue((new SqlParameter("@" + columnname, GuidObj)), i);
                        }
                        else
                            ParameterName.SetValue((new SqlParameter("@" + columnname, OriginalDataReader[columnname])), i);

                        i++;
                        columnnames += "@" + columnname + ",";
                    }
                    if (columnnames != null)
                    {
                        columnnames = "( " + columnnames.Substring(0, columnnames.Length - 1) + " )";
                        if (!ParameterName.All(item => item == null))
                        {
                            try
                            {
                                replicadatabasecontext.Database.ExecuteSqlCommand("insert into " + tbname + " values " + columnnames, ParameterName);
                            }
                            catch (Exception e)
                            {
                                if (e.Message.Contains("duplicate key") || e.Message.Contains("Violation of PRIMARY KEY"))
                                    duplicateIdError += OriginalDataReader["Id"] + ",";
                                else
                                    ErrorId += OriginalDataReader["Id"] + ",";
                            }
                        }
                    }
                }
                replicadatabasecontext.Database.CloseConnection();
                origindatabasecontext.Database.CloseConnection();

                if (duplicateIdError.Last() == ',')
                    duplicateIdError = duplicateIdError.Substring(0, duplicateIdError.Length - 1);

                if (ErrorId.Last() == ',')
                    ErrorId = ErrorId.Substring(0, ErrorId.Length - 1);

                duplicateIdError += "),";
                ErrorId += "),";
            }
        }
    }
}
