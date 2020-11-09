using System;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace TrainTimesToSQL
{
    //Console application that will get train times for given station, for given days and times using TransportAPI.com

    class JsonAttributes
    {
        //initialise JsonAttributes class to store a dictionary for retrived json results in the GetStationTimes Method
        public List<Dictionary<string, string>> Stops { get; set; }

    }

    class DBCreation
    {
        //initialise relevant variables that will be used throughout this class.

        //Transport API variables.
        string appID = ""; //Your app ID goes here
        string appKey = ""; //Your app key goes here 


        //List of dates that the API calls will iterate through. Only one is here at the moment.
        string[] dates = new string[1] { "2020-11-11"};

        //Every hour to be used with the API call. I tried to set the API call so it will give me all times for 24 hours from a given day from 00:00 but it didn't seem to work.

        string[] hours = new string[24] {"00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00",
                                            "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00",
                                            "19:00", "20:00", "21:00", "22:00", "23:00"};

        //initialise the station list. These stations will be read from a text file. 
        public List<string> stationList = new List<string>();

        //initialise the connection string to connect to the remote SQL server hosted in Azure. 

        string connectionString = @""; //Your SQL server connection string goes here 



      

        public void ConnectDb()
        {
            //This method will connect to the remote SQL server and create an empty table ready for the station times to be added to. 

            //If the table does not already exist, then it will be created. 

            //Sql command text

           


            string command = @"IF NOT EXISTS (SELECT * FROM sysobjects 
                        WHERE NAME='DepartTimes2' AND xtype='U') CREATE TABLE DepartTimes2 (from_station varchar(64), to_station varchar(64), depart_time varchar(64), arrive_time varchar(64), datestamp varchar(64))";

            try
            {
                //Open a new Sql connection
                using (SqlConnection conn = new SqlConnection(connectionString))

                {
                    conn.Open();
                    Console.WriteLine("DB Connection Success");

                }
            }
            //If connection is unsuccessful, then a catch exception error will be outputed to the console.
            catch (Exception ex)
            {
                Console.WriteLine("DB Connection Fail");
                Console.WriteLine(ex.Message);
            }

            try
            {
                //Execute the command of creating a table for the station times.
                using (SqlConnection conn = new SqlConnection(connectionString))

                {
                    conn.Open();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = command;
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("DepartTimes database sucesfully created");
                }
            }
            //If the table is not sucesfully created, catch the exception error and print it to the console.
            catch (Exception ex)
            {
                Console.WriteLine("Error: DepartTimes database not created");
                Console.WriteLine(ex.Message);
            }

        }


        public List<string> ReadStationFileList()
        {

            //This method will read a text file that has a list of station CRS codes and will add each station code to the station list initialised at
            //the begining of this class. 

            //the text file is in the default directory of this project.

            StreamReader fileReader = new StreamReader(@"stations.txt");
            string lineReadFromFile = fileReader.ReadLine();

            while (lineReadFromFile != null)
            {
                //while each line from the text file is not null, then the line will be read.

                //add each line to stationList
                stationList.Add(lineReadFromFile);

                lineReadFromFile = fileReader.ReadLine();
            }

            //return the station list for future use.
            return stationList;
        }





        public string get_selected_local_departs(string url)
        {
            //This method will create URI from the Transport API call that is used in the following method.
            //This is to parse the JSON data.

            Uri uri = new Uri(url);
            HttpWebRequest request = (HttpWebRequest)HttpWebRequest.Create(uri);
            request.Method = WebRequestMethods.Http.Get;
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();
            StreamReader reader = new StreamReader(response.GetResponseStream());
            string output = reader.ReadToEnd();
            response.Close();

            return output;
        }

        public void GetStationTimes(string stationCode)
        {

            //This method will make API calls to transport API and will insert the relevant information into the DepartTimes database created prior to this method.           
            //Information gathered consists of all train departures on a given day, and where each departure will be calling at. From a given station code.

            //Sql command text, to insert the required fields that will be specifed from line 211, into the DepartTimes table.

         


            string insertTimesIntoDb = "INSERT INTO DepartTimes2 (from_station, to_station, depart_time, arrive_time, datestamp) VALUES (@from_station, @to_station, @depart_time, @arrive_time, @datestamp)";

            //loop through each date and time in both lists. A for loop within a foor loop can cause poor performance so I will get back to 
            //revising this. Maybe using a parallel for loop.

            foreach (var date in dates)
            {
                foreach (var time in hours)
                {
                    //This string contains the API call URL with the relevant optional paramaters present in the URL.
                    string localSelectedJson = get_selected_local_departs("https://transportapi.com/v3/uk/train/station/" + stationCode + "/" + date + "/" + time + "/timetable.json?app_id=" + appID + "&app_key=" + appKey + "&to_offset=PT01:00:00&train_status=passenger");

                    //Create dynamic arrray to store deserialized json attributes.
                    dynamic localStationArray = JsonConvert.DeserializeObject(localSelectedJson);

                    //following 2 dynamic variables are part of the json schema. This will allow access to json child attributes.
                    dynamic departures = localStationArray.departures;
                    dynamic localSelectedStationDeparts = departures.all;

                    //loop through each json value within the departures node. 
                    foreach (var depart in localSelectedStationDeparts)
                    {
                        //within each departure, there is a link to the service timetable. This service time table contains 
                        //the information of stations the serive is calling at. This link will be used in another API call.

                        string getServices = depart["service_timetable"]["id"];

                        //New API call to the service time table link.
                        string localselectedjson2 = get_selected_local_departs(getServices);

                        //Create another dynamic arrray to store the new  deserialized json attributes.

                        var stops = JsonConvert.DeserializeObject<JsonAttributes>(localselectedjson2).Stops;


                        //Count the number of stops in the service timetable .
                        int stopsLength = stops.Count();

                        //Make a stop counter so when the given station is reached, a for loop can count from stopsCounter to the stopsLength.
                        //This will get all calling stations after the given station code.
                        int stopsCounter = 0;

                        //foreach item in the service timetable
                        foreach (var depart2 in stops)
                        {
                            //increment the counter until the given staiton is reached.
                            stopsCounter += 1;

                            if (depart2["station_code"] == stationCode)
                            {
                                //when the given station is reached, count from there to the total number of stops there are
                                //in the service timtable to get all stops to get all calling stations
                                for (int x = stopsCounter + 1; x < stopsLength; x++)
                                {

                                    //Execute the Sql command to insert the values into the database. 
                                    using (SqlConnection conn = new SqlConnection(connectionString))

                                    {
                                        try
                                        {
                                            conn.Open();
                                            SqlCommand cmd = new SqlCommand(insertTimesIntoDb, conn);

                                            //Give the paramaters the relevant values that have been retirved from the json attributes.

                                            cmd.Parameters.AddWithValue("@from_station", depart2["station_code"]);
                                            cmd.Parameters.AddWithValue("@to_station", stops[x]["station_code"]);
                                            cmd.Parameters.AddWithValue("@depart_time", depart2["aimed_departure_time"] + ":00");
                                            cmd.Parameters.AddWithValue("@arrive_time", stops[x]["aimed_arrival_time"] + ":00");
                                            cmd.Parameters.AddWithValue("@datestamp", date);

                                            //Execute query.
                                            cmd.ExecuteNonQuery();
                                        }
                                        finally
                                        {
                                            //Close the connection
                                            conn.Close();
                                        }

                                        //Write the results to the console so the user can see something is going on. 
                                        Console.WriteLine(depart2["station_code"] + " " + stops[x]["station_code"] + "\t " + depart2["aimed_departure_time"] + ":00" + "\t" + stops[x]["aimed_arrival_time"] + ":00" + "\t" + date);
                                    }
                                }
                            }
                        }
                    }
                    //Blank line in between each service time table. 
                    Console.WriteLine();

                }
            }
        }


        public void CreateTableStationCodeDepartures(string stationCode)
        {
            //Once all the information for the departures has been gathered, this method will create an empty table for each table that is in the station code list.
            //Departures for each indiviual station will inserted into these tables. This will be later used for a inner join SQL query to determine where one can change
            //in order to get to a given detination. 

           

            string commandText = @"CREATE TABLE " + stationCode + "_DepartTimes (from_station varchar(64), to_station varchar(64), depart_time varchar(64), arrive_time varchar(64), datestamp varchar(64))";

            try
            {
                //Open SQl Connection.
                using (SqlConnection conn = new SqlConnection(connectionString))

                {
                    conn.Open();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = commandText;
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Table created for station code" + stationCode);
                }
            }
            //If table creation is unsuccessful, catch exception error and display to user in console. 
            catch (Exception ex)
            {
                Console.WriteLine("Error: Table not created for station code " + stationCode);
                Console.WriteLine(ex.Message);
            }


        }




        public void StationCodeDepartures(string stationCode)
        {
            //Following on from the previous method, this method will insert the values into the relevant table for
            //departures from the station code.

          


            string newStationCode = stationCode + "_DepartTimes";

            string commandText = @"INSERT INTO " + stationCode + "_DepartTimes SELECT * FROM DepartTimes WHERE from_station = @stationCode";



            try
            {
                //open Sql Connection
                using (SqlConnection conn = new SqlConnection(connectionString))

                {
                    conn.Open();
                    var cmd = conn.CreateCommand();
                    cmd.CommandText = commandText;
                    cmd.Parameters.AddWithValue("@stationCode", stationCode);
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Station times inserted  for station code " + stationCode);
                }
            }
            //If unsuccessful, display error message in console.
            catch (Exception ex)
            {
                Console.WriteLine("Error: Station times table not created for " + stationCode);
                Console.WriteLine(ex.Message);
            }

        }





        static void Main(string[] args)
        {
            DBCreation getTrainTimes = new DBCreation();
            getTrainTimes.ConnectDb();
            //Call the ConnectDb method to create an empty table for the results to be inserted into.
            //getTrainTimes.Test();

            //Parallel foreach loop to make API calls for each station code in the list. Parallel for loop is considerably faster than
            //Normal for loop.

            Parallel.ForEach(getTrainTimes.ReadStationFileList(), (currentStation) =>
            {
                //The station code list is returned from the ReadStationFileList method

                getTrainTimes.GetStationTimes(currentStation);
            });

        }
    }

}

