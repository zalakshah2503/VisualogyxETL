using System;
using System.Collections.Generic;
using System.Linq;
using Npgsql;
using System.Data.SqlClient;
using NpgsqlTypes;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;

namespace VisualogyxETL
{
    class Program
    {
        static void Main(string[] args)
        {
            //Get Provider Name
            Console.WriteLine("Write Provide Name :- ");
            string providerName = Console.ReadLine();
            string insertQuery;
            string[] tableNameList = null;
            DbConnection sourceConnection = null, destinationConnection = null;

            if (providerName == "PostgreSQL")
            {
                //List of Tables
                string[] tablesList = { "MembershipPlans", "Tags", "Settings", "Invitations", "Users", "UserImages", "UserTagMappings", "Team", "TeamInvitations", "TeamUserMapping", "Jobs", "JobTransfer", "Group", "Corpacs", "Contacts", "TaskAdditionalData", "ContactUserMappings", "JobRequestUserMappings", "GroupUserMapping", "ContactTeamMappings", "ContactEmailMappings", "ContactJobMappings", "ContactPhoneMappings", "ContactTagMappings" };
                //string[] tablesList = { "Tasks" }; // Remaining Table
                tableNameList = tablesList;

                //add connectionstrings of source & destination databases
                sourceConnection = new NpgsqlConnection(@"User ID=postgres;Password=P@ssw0rd;Host=server12;Port=5433;Database=visualogyxOld;Pooling=true;");
                destinationConnection = new NpgsqlConnection(@"User ID=postgres;Password=P@ssw0rd;Host=server12;Port=5433;Database=VisualogyxNew;Pooling=true;");
            }
            else if (providerName == "System.Data.SqlClient")
            {
                //List of Tables
                string[] tablesList = { "UserInfo", "Department", "Job", "Progress" };
                tableNameList = tablesList;

                //add connectionstrings of source & destination databases
                sourceConnection = new SqlConnection(@"Server=(localdb)\mssqllocaldb;Database=OriginDatabase;Trusted_Connection=True;");
                destinationConnection = new SqlConnection(@"Server=(localdb)\mssqllocaldb;Database=ReplicaDatabase;Trusted_Connection=True;");
            }
            else if (providerName == "SQLite")
            {
                //List of Tables
                string[] tablesList = { "UserInfo", "Department", "Job", "Progress" };
                tableNameList = tablesList;

                //add connectionstrings of source & destination databases
                sourceConnection = new SqliteConnection();
                destinationConnection = new SqliteConnection();
            }
            else if (providerName == "MySQL")
            {
                //List of Tables
                string[] tablesList = { "UserInfo", "Department", "Job", "Progress" };
                tableNameList = tablesList;

                //add connectionstrings of source & destination databases
                sourceConnection = new MySqlConnection();
                destinationConnection = new MySqlConnection();
            }
            using (sourceConnection)
            using (destinationConnection)
            {
                foreach (string tbName in tableNameList)
                {
                    insertQuery = ""; //reset data

                    //Make Query for fetch data
                    string query = "select * from ";
                    if (providerName == "PostgreSQL")
                    {
                        query += "public.\"" + tbName + "\"";
                        if (tbName == "Jobs")
                        {
                            query = "SELECT *  FROM (SELECT *,ROW_NUMBER() OVER(PARTITION BY \"JobKey\" ORDER BY \"Id\" DESC) rn FROM public.\"Jobs\")as a WHERE rn = 1";
                        }
                        else
                            query += " where \"JobId\" in (SELECT \"Id\"  FROM ( SELECT \"Id\",ROW_NUMBER() OVER(PARTITION BY \"JobKey\" ORDER BY \"Id\" DESC) rn FROM public.\"Jobs\")as a WHERE rn = 1 )";
                    }
                    else if (providerName == "System.Data.SqlClient" || providerName == "SQLite" || providerName == "MySQL")
                    {
                        query += tbName;
                    }

                    //Create Command with current connection
                    var sourceCommand = sourceConnection.CreateCommand();
                    var destinationCommand = destinationConnection.CreateCommand();
                    var destinationInsertCommand = destinationConnection.CreateCommand();

                    sourceCommand.CommandText = query;
                    destinationCommand.CommandText = query;

                    //Change State of Connection to open
                    if (sourceConnection.State != System.Data.ConnectionState.Open)
                        sourceConnection.Open();
                    if (destinationConnection.State != System.Data.ConnectionState.Open)
                        destinationConnection.Open();

                    //Retrieving Data Using a DataReader
                    using (var sourceResult = sourceCommand.ExecuteReader())
                    using (var destinationResult = destinationCommand.ExecuteReader())
                    {
                        //Retrieving ColumnNames and/or DataType
                        var columnNamesWithDataTypeList = destinationResult.GetColumnSchema().Select(o => new { ColumnName = o.ColumnName, DataTypeName = o.DataTypeName }).OrderBy(x => x.ColumnName).ToList();
                        IEnumerable<string> columnNamesList = sourceResult.GetColumnSchema().Select(o => new { ColumnName = o.ColumnName }).OrderBy(x => x.ColumnName).Select(a => a.ColumnName);

                        while (sourceResult.Read())
                        {
                            //Reset Data
                            int i = 0;
                            string columnNameValues = "", columnNames = "";

                            foreach (var columnname in columnNamesList)
                            {
                                if (tbName == "Jobs" && columnname == "rn")
                                {
                                    i++;
                                    continue;
                                }
                                if (tbName == "Tasks" && columnname == "JobId") i++;
                                if (tbName == "Tasks" && columnname == "TaskTypeStr") continue;

                                //Check datatype of column is GUID is not in new Database
                                if (columnNamesWithDataTypeList[i].DataTypeName == "uuid" || columnNamesWithDataTypeList[i].DataTypeName == "uniqueidentifier" || columnNamesWithDataTypeList[i].DataTypeName == "blob" || columnNamesWithDataTypeList[i].DataTypeName == "Guid") // && columnNamesWithDataTypeList[i].ColumnName == columnname
                                {
                                    if (sourceResult[columnname].ToString() != "")
                                    {
                                        //Convert int to GUID
                                        byte[] bytes = new byte[16];
                                        BitConverter.GetBytes(Convert.ToInt32(sourceResult[columnname])).CopyTo(bytes, 0);
                                        Guid guidObj = new Guid(bytes);

                                        //Set parameter as per column
                                        if (providerName == "PostgreSQL")
                                            destinationInsertCommand.Parameters.Add(new NpgsqlParameter("@" + columnname + destinationInsertCommand.Parameters.Count, guidObj));
                                        else if (providerName == "System.Data.SqlClient")
                                            destinationInsertCommand.Parameters.Add(new SqlParameter("@" + columnname + destinationInsertCommand.Parameters.Count, guidObj));
                                        else if (providerName == "SQLite")
                                            destinationInsertCommand.Parameters.Add(new SqliteParameter("@" + columnname + destinationInsertCommand.Parameters.Count, guidObj));
                                        else if (providerName == "MySQL")
                                            destinationInsertCommand.Parameters.Add(new MySqlParameter("@" + columnname + destinationInsertCommand.Parameters.Count, guidObj));

                                    }
                                    else
                                    {
                                        i++;
                                        continue;
                                    }
                                }
                                else
                                {
                                    //Set parameter as per column
                                    if (providerName == "PostgreSQL")
                                    {
                                        NpgsqlParameter npgsqlParameter = new NpgsqlParameter();
                                        npgsqlParameter.ParameterName = "@" + columnname + destinationInsertCommand.Parameters.Count;
                                        if (columnname == "UpdatedDateTime" && sourceResult[columnname].ToString() == "")
                                        {
                                            npgsqlParameter.Value = sourceResult["CreatedDateTime"];
                                        }
                                        else if (columnNamesWithDataTypeList[i].DataTypeName == "jsonb")
                                        {
                                            npgsqlParameter.Value = sourceResult[columnname];
                                            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.Jsonb;
                                        }
                                        else if (columnNamesWithDataTypeList[i].DataTypeName == "float4")
                                        {
                                            npgsqlParameter.Value = sourceResult[columnname];
                                            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.Real;
                                        }
                                        else
                                        {
                                            npgsqlParameter.Value = sourceResult[columnname];
                                        }
                                        destinationInsertCommand.Parameters.Add(npgsqlParameter);
                                    }
                                    else if (providerName == "System.Data.SqlClient")
                                    {
                                        SqlParameter sqlParameter = new SqlParameter();
                                        sqlParameter.ParameterName = "@" + columnname + destinationInsertCommand.Parameters.Count;
                                        sqlParameter.Value = sourceResult[columnname];
                                        destinationInsertCommand.Parameters.Add(sqlParameter);
                                    }
                                    else if (providerName == "SQLite")
                                    {
                                        SqliteParameter sqliteParameter = new SqliteParameter();
                                        sqliteParameter.ParameterName = "@" + columnname + destinationInsertCommand.Parameters.Count;
                                        sqliteParameter.Value = sourceResult[columnname];
                                        destinationInsertCommand.Parameters.Add(sqliteParameter);
                                    }
                                    else if (providerName == "MySQL")
                                    {
                                        MySqlParameter mysqlParameter = new MySqlParameter();
                                        mysqlParameter.ParameterName = "@" + columnname + destinationInsertCommand.Parameters.Count;
                                        mysqlParameter.Value = sourceResult[columnname];
                                        destinationInsertCommand.Parameters.Add(mysqlParameter);
                                    }
                                }

                                //Make string of columns for insert query
                                columnNameValues += "@" + columnname + (destinationInsertCommand.Parameters.Count - 1) + ", ";
                                columnNames += "\"" + columnNamesWithDataTypeList[i].ColumnName + "\"" + ", ";
                                i++;
                            }
                            if (columnNameValues != null && columnNames != null)
                            {
                                columnNameValues = "( " + columnNameValues.Substring(0, columnNameValues.Length - 2) + " )";
                                columnNames = "( " + columnNames.Substring(0, columnNames.Length - 2) + " )";
                                //Check if All Parameters are null or not and make query for insert data provider wise
                                if (providerName == "PostgreSQL")
                                {
                                    if (!destinationInsertCommand.Parameters.Cast<NpgsqlParameter>().All(item => item == null))
                                    {
                                        insertQuery += "insert into public.\"" + tbName + "\" " + columnNames + " values " + columnNameValues + "; ";
                                    }
                                }
                                else if (providerName == "System.Data.SqlClient")
                                {
                                    if (!destinationInsertCommand.Parameters.Cast<SqlParameter>().All(item => item == null))
                                    {
                                        insertQuery += "insert into " + tbName + " " + columnNames + " values " + columnNameValues + "; ";
                                    }
                                }
                                else if (providerName == "SQLite")
                                {
                                    if (!destinationInsertCommand.Parameters.Cast<SqliteParameter>().All(item => item == null))
                                    {
                                        insertQuery += "insert into " + tbName + " " + columnNames + " values " + columnNameValues + "; ";
                                    }
                                }
                                else if (providerName == "MySQL")
                                {
                                    if (!destinationInsertCommand.Parameters.Cast<MySqlParameter>().All(item => item == null))
                                    {
                                        insertQuery += "insert into " + tbName + " " + columnNames + " values " + columnNameValues + "; ";
                                    }
                                }
                            }
                        }
                    }

                    try
                    {
                        //Insert data into table of new database and get Affected rows
                        destinationInsertCommand.CommandText = insertQuery;

                        int insertedRow = destinationInsertCommand.ExecuteNonQuery();
                        Console.WriteLine(insertedRow + " row(s) inserted in table - " + tbName);
                    }
                    catch (Exception e)
                    {
                        //Print Error                    
                        Console.WriteLine(e.Message);
                    }
                }
            }
            Console.Read();
        }
    }
}
