using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Web;
using System.Web.Configuration;
using System.Web.Script.Serialization;

namespace TKDS.Library
{
    public class Database
    {
        public static string GetConnectionString()
        {
            return WebConfigurationManager.ConnectionStrings[""].ConnectionString.ToString();
        }

        public static DataSet GetDataSet(string sqlCommand)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand cmd = new SqlCommand(sqlCommand, connection);

                var ds = new DataSet();

                try
                {
                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    var da = new SqlDataAdapter(cmd);
                    da.SelectCommand.CommandTimeout = 300;

                    da.Fill(ds);

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                }
                catch
                {

                }

                return ds;
            }
        }

        public static DataSet GetDataSetParameter(string sqlCommand, SqlParameter[] sqlParameters)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand cmd = new SqlCommand(sqlCommand, connection);

                var ds = new DataSet();

                try
                {
                    if (sqlParameters != null)
                    {
                        foreach (var parameter in sqlParameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                    }

                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    var da = new SqlDataAdapter(cmd);
                    da.SelectCommand.CommandTimeout = 300;

                    da.Fill(ds);

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                }
                catch
                {

                }

                return ds;
            }
        }

        /// <summary>
        /// Returns a first DataTable of the DataSet.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns>First DataTable in DataSet</returns>
        public static DataTable GetDataTable(string sqlCommand)
        {
            return GetDataSet(sqlCommand).Tables[0];
        }

        public static DataTable GetDataTableParameter(string sqlCommand, SqlParameter[] sqlParameters)
        {
            return GetDataSetParameter(sqlCommand, sqlParameters).Tables[0];
        }

        /// <summary>
        /// Gets the SQL command and retrieves a list as type of string with comma delimiter
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public static List<string> GetCommaSeperatedList(string sqlCommand)
        {
            DataTable dt = GetDataTable(sqlCommand);
            List<string> list = new List<string>();

            foreach (DataRow row in dt.Rows)
            {
                StringBuilder sb = new StringBuilder();

                foreach (DataColumn column in dt.Columns)
                {
                    sb.Append(row[column].ToString()).Append(",");
                }

                sb.Remove(sb.Length - 1, 1);

                list.Add(sb.ToString());
            }

            return list;
        }

        /// <summary>
        /// Gets the SQL command and retrieves a list as type of DataRow
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public static List<DataRow> GetList(string sqlCommand)
        {
            DataTable dt = GetDataTable(sqlCommand);
            List<DataRow> list = dt.AsEnumerable().ToList<DataRow>();
            /*List<DataRow> list = new List<DataRow>();

            foreach (DataRow row in dt.Rows)
            {
                list.Add(row);
            }*/

            return list;
        }

        //SetParameter: Stored procedure için parametre hazırlar. Yönü string bir değerle
        //verilir. Daha sonra, alınan bu değer Direction değerlerinden birine çevrilir.
        //Geriye bir SqlParameter çevrilir.
        // "parameterName": Parametre adı.
        // "dbType": Parametre tipi.
        // "iSize": Parametre boyu.
        // "direction": Parametrenin yönü.
        // "oParamValue": Parametrenin değeri.

        public static SqlParameter SetParameter(string parameterName, SqlDbType dbType, Int32 iSize, string direction, object oParamValue)
        {
            var parameter = new SqlParameter(parameterName, dbType, iSize);

            switch (direction)
            {
                case "Input":
                    parameter.Direction = ParameterDirection.Input;
                    break;
                case "Output":
                    parameter.Direction = ParameterDirection.Output;
                    break;
                case "ReturnValue":
                    parameter.Direction = ParameterDirection.ReturnValue;
                    break;
                case "InputOutput":
                    parameter.Direction = ParameterDirection.InputOutput;
                    break;
                default:
                    break;
            }

            parameter.Value = oParamValue;
            return parameter;
        }


        public static SqlParameter SetImgParameter(string parameterName, SqlDbType dbType, Int32 iSize, object oParamValue)
        {
            var parameter = new SqlParameter(parameterName, dbType, iSize);



            parameter.Value = oParamValue;
            return parameter;
        }

        /// <summary>
        /// This method execute query string with parameters
        /// </summary>
        /// <param name="sql">Your query</param>
        /// <param name="sqlParameters">parameters of query</param>
        /// <returns></returns>
        public static DataTable ExecuteSqlWithParameters(string sql, SqlParameter[] sqlParameters)
        {

            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand cmd = new SqlCommand(sql, connection);
                try
                {
                    if (sqlParameters != null)
                    {
                        foreach (var parameter in sqlParameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                    }

                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }


                    var da = new SqlDataAdapter(cmd);

                    da.SelectCommand.CommandTimeout = 300;

                    var ds = new DataSet();
                    da.Fill(ds);

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return ds.Tables.Count != 0 ? ds.Tables[0] : new DataTable();
                }
                catch (SqlException ex)
                {
                    try
                    {

                    }
                    catch (SqlException eXR)
                    {
                        throw eXR;
                    }
                    throw ex;
                }
            }
        }

        /// <summary>
        /// This method execute query string with parameters
        /// </summary>
        /// <param name="sql">Your query</param>
        /// <param name="sqlParameters">parameters of query</param>
        /// <returns></returns>
        public static int ExecuteNonQueryWithParameters(string sql, SqlParameter[] sqlParameters)
        {
            int result = 0;
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand cmd = new SqlCommand(sql, connection);
                try
                {
                    if (sqlParameters != null)
                    {
                        foreach (var parameter in sqlParameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                    }


                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    cmd.CommandTimeout = 300;


                    result = cmd.ExecuteNonQuery();


                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return result;
                }
                catch (SqlException ex)
                {
                    try
                    {

                    }
                    catch (SqlException eXR)
                    {
                        throw eXR;
                    }
                    throw ex;
                }
            }
        }

        /// <summary>
        /// This method executes only stored procedure with parameters of sp
        /// </summary>
        /// <param name="procedureName">StoredProcedure name</param>
        /// <param name="sqlParameters">Parameters of StoredProcedure</param>
        /// <param name="returnValueParameter">Stored Procedure that we have defined in the name of the return parameter.</param>
        /// <returns></returns>
        public static int ExecuteStoredProcedure(string procedureName, SqlParameter[] sqlParameters, string returnValueParameter)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand cmd = new SqlCommand(procedureName, connection);
                cmd.CommandType = CommandType.StoredProcedure;
                try
                {
                    if (sqlParameters != null)
                    {
                        foreach (var parameter in sqlParameters)
                        {
                            cmd.Parameters.Add(parameter);
                        }
                    }

                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    cmd.CommandTimeout = 300;

                    cmd.ExecuteNonQuery();

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return Convert.ToInt32(cmd.Parameters[returnValueParameter].Value);
                }
                catch (SqlException ex)
                {
                    try
                    {

                    }
                    catch (SqlException eXR)
                    {
                        throw eXR;
                    }
                    throw ex;
                }
            }
        }

        /// <summary>
        /// var sqlParameters = new SqlParameter[6];
        /// sqlParameters[0] = DataBase.SetParameter("@parameterName", SqlDbType.VarChar, 32, "Input", Session["parameter"]);
        /// var dt = DataBase.ExecuteStoredProcedure2DataTable("GetSomeList", sqlParameters);
        /// </summary>
        /// <param name="procedureName"></param>
        /// <param name="sqlParameters"></param>
        /// <returns></returns>
        public static DataTable ExecuteStoredProcedure2DataTable(string procedureName, SqlParameter[] sqlParameters)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlTransaction transaction = connection.BeginTransaction();
                SqlCommand command = new SqlCommand(procedureName, connection, transaction);
                command.CommandType = CommandType.StoredProcedure;
                try
                {
                    if (sqlParameters != null)
                    {
                        foreach (var parameter in sqlParameters)
                        {
                            command.Parameters.Add(parameter);
                        }
                    }

                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }

                    var da = new SqlDataAdapter(command);

                    da.SelectCommand.CommandTimeout = 300;

                    var ds = new DataSet();
                    da.Fill(ds);
                    command.Transaction.Commit();

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return ds.Tables.Count != 0 ? ds.Tables[0] : new DataTable();
                }
                catch (SqlException eX)
                {
                    try
                    {
                        command.Transaction.Rollback();
                    }
                    catch (SqlException eXR)
                    {
                        throw eXR;
                    }
                    throw eX;
                }
            }
        }

        /// <summary>
        /// Executes a SQL query as nonquery.
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public static int ExecuteNonQuery(string sqlCommand)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {

                try
                {
                    if (connection.State == ConnectionState.Closed)
                    {
                        connection.Open();
                    }
                    SqlCommand command = new SqlCommand(sqlCommand, connection);
                    command.CommandTimeout = 500;


                    var result = command.ExecuteNonQuery();

                    if (connection.State == ConnectionState.Open)
                    {
                        connection.Close();
                        connection.Dispose();
                    }

                    return result;
                }
                catch (SqlException eX)
                {
                    try
                    {
                        SqlCommand command = new SqlCommand(sqlCommand, connection);
                    }
                    catch (SqlException eXR)
                    {
                        throw eXR;
                    }
                    throw eX;
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="sqlCommand">Query</param>
        /// <returns>First cell[0,0]</returns>
        public static object ExecuteScalar(string sqlCommand)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand command = new SqlCommand(sqlCommand, connection);
                //try
                //{
                if (connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }

                command.CommandTimeout = 300;

                var result = command.ExecuteScalar();

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    connection.Dispose();
                }

                return result;
                //}
                //catch (SqlException eX)
                //{
                //    try
                //    {
                //        //command.Transaction.Rollback();
                //    }
                //    catch (SqlException eXR)
                //    {
                //        throw eXR;
                //    }
                //    throw eX;
                //}
            }
        }

        public static object ExecuteScalarParameters(string sqlCommand, SqlParameter[] sqlParameters)
        {
            using (SqlConnection connection = new SqlConnection(GetConnectionString()))
            {
                SqlCommand command = new SqlCommand(sqlCommand, connection);
                //try
                //{
                if (sqlParameters != null)
                {
                    foreach (var parameter in sqlParameters)
                    {
                        command.Parameters.Add(parameter);
                    }
                }

                if (connection.State == ConnectionState.Closed)
                {
                    connection.Open();
                }

                command.CommandTimeout = 300;

                var result = command.ExecuteScalar();

                if (connection.State == ConnectionState.Open)
                {
                    connection.Close();
                    connection.Dispose();
                }

                return result;
            }
        }

        /// <summary>
        /// Removes ' character
        /// </summary>
        /// <param name="sqlCommand"></param>
        /// <returns></returns>
        public static string CleanString(string sqlCommand)
        {
            return sqlCommand.Replace("'", "''");
        }


        public static DataTable JSONToDataTable(string JSON)
        {
            JavaScriptSerializer jss = new JavaScriptSerializer();
            DataTable dt = new DataTable();
            List<Dictionary<string, string>> list = jss.Deserialize<List<Dictionary<string, string>>>(JSON);

            foreach (KeyValuePair<string, string> kvp in list[0])
            {
                dt.Columns.Add(kvp.Key);
            }
            foreach (Dictionary<string, string> dic in list)
            {
                DataRow dr = dt.NewRow();

                foreach (KeyValuePair<string, string> kvp in dic)
                {
                    dr[kvp.Key] = kvp.Value;
                }

                dt.Rows.Add(dr);
            }

            return dt;
        }

        public static string DataTableToJSON(DataTable table)
        {
            StringBuilder sb = new StringBuilder();
            foreach (DataRow dr in table.Rows)
            {
                if (sb.Length != 0)
                {
                    sb.Append(",");
                }

                sb.Append("{");
                StringBuilder sb2 = new StringBuilder();

                foreach (DataColumn col in table.Columns)
                {
                    string fieldname = col.ColumnName;
                    string fieldvalue = dr[fieldname].ToString();
                    if (sb2.Length != 0)
                    {
                        sb2.Append(",");
                    }

                    sb2.Append(string.Format("\"{0}\":\"{1}\"", fieldname, fieldvalue));
                }

                sb.Append(sb2.ToString());
                sb.Append("}");

            }

            sb.Insert(0, "[");
            sb.Append("]");

            return sb.ToString();
        }
    }
}