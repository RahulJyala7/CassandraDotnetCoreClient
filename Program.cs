using System;
using Cassandra;

namespace CassandraDemo
{
    class Program
    {
        public static string NODE_URL = "192.168.0.148";
        static void Main()
        {
            Console.WriteLine("--Select an Option--");
            Console.WriteLine("1: Show KeySpaces");
            Console.WriteLine("2: Create KeySpaces");
            Console.WriteLine("3: Create Table");
            Console.WriteLine("4: Show Table");

            var Key = Console.ReadLine();

            if (string.IsNullOrEmpty(Key))
            {
                Main();
            }
            else
            {
                InitCassandra(Key);
            }
        }

        static void InitCassandra(string key)
        {
            try
            {
                var cluster = Cluster.Builder()
                         .AddContactPoints(NODE_URL)
                         .Build();

                switch (Convert.ToInt16(key))
                {
                    case 1:
                        var keyspaces = cluster.Metadata.GetKeyspaces();
                        foreach (var keyspaceName in keyspaces)
                        {
                            Console.WriteLine(keyspaceName);
                        }
                        break;

                    case 2:
                        Console.WriteLine("Create KeySpace");
                        break;

                    case 3:
                        //Connect to the nodes using a keyspace
                        var session = cluster.Connect("druid");
                        // Execute a query on a connection synchronously
                        //var rs = session.Execute("CREATE TABLE descriptor_storage(key varchar,lastModified timestamp,descriptor varchar,PRIMARY KEY (key)) WITH COMPACT STORAGE;");
                        //Console.WriteLine(rs.Info);
                        break;

                    case 4:
                        var keyspaces1 = cluster.Metadata.GetTables("druid");
                        foreach (var item in keyspaces1)
                        {
                            Console.WriteLine(item);
                        }
                        break;

                    default:
                        Console.WriteLine("--Not Valid Option Press Any Key--");
                        break;
                }
                Console.ReadLine();
                Main();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }
}
