using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using ECOMMS_Client;
using ECOMMS_Entity;
using ECOMMS_Manager;
using System.Text.Json;


using InfluxDB.Collector;
using InfluxDB.LineProtocol.Client;
using InfluxDB.LineProtocol.Payload;

namespace ecomms_simple_influxdb_service
{
    //copied from ecomms_io solution until i create a shared project
    public class SensorData
    {
        public String name { get; set; }
        public String temperature { get; set; }
        public String description { get; set; }
        public String location { get; set; }
        public String high { get; set; }
        public String low { get; set; }

        public IClient client { get; set; }

        public SensorData()
        {
        }
    }

    //copied from ecomms_io solution until i create a shared project
    public class SensorDataPoint
    {
        public int temperature { get; set; }
        public int humidity { get; set; }
        public float level { get; set; }
    }


    class Program
    {
        static List<string> _sensorNames = new List<string>();
        static List<SensorData> _sensorDataList = new List<SensorData>();
        static Dictionary<string, SensorData> _sensorDictionary = new Dictionary<string, SensorData>();

        private async static void add(IClient client, string status)
        {
            SensorDataPoint sdp = JsonSerializer.Deserialize<SensorDataPoint>(status);


            var cpuTime = new LineProtocolPoint(
                "sensors",
                new Dictionary<string, object>
                {
                    { "temperature", sdp.temperature.ToString() },
                    { "humidity", sdp.humidity.ToString() }
                },

                new Dictionary<string, string>
                {
                    { "sensor", client.name },
                    { "location", _sensorDictionary[client.name].location }
                },

                DateTime.UtcNow);

            var payload = new LineProtocolPayload();
            payload.Add(cpuTime);

            var influx = new LineProtocolClient(new Uri("http://192.168.86.30:8086"), "firstdb");
            var influxResult = await influx.WriteAsync(payload);

            if (!influxResult.Success)
                Console.Error.WriteLine(influxResult.ErrorMessage);
        }

        //add a sensor to our list of sensors
        private static void addSensor(IClient client)
        {
            if (client.role == Role.Sensor)
            {

                Console.WriteLine(client.name + " SENSOR ADDED");

                if (!_sensorNames.Contains(client.name))
                {
                    _sensorNames.Add(client.name);
                    _sensorDataList.Add(new SensorData());
                    _sensorDictionary.Add(client.name, new SensorData());

                    _sensorDictionary[client.name].client = client;

                    //get the location
                    client.doGet("location", (response) =>
                    {
                        _sensorDictionary[client.name].location = response;
                    });

                    //get the low
                    client.doGet("low", (response) =>
                    {
                        _sensorDictionary[client.name].low = response;
                    });

                    //get the high
                    client.doGet("high", (response) =>
                    {
                        _sensorDictionary[client.name].high = response;
                    });

                    //listen for run state changes
                    client.addObserver(new ObserverAdapterEx((anobject, hint, data) =>
                    {
                        Console.WriteLine((hint as string));
                    }));

                    client.addObserver(new ObserverAdapter((observable, hint) =>
                    {
                        String notification = hint as String;

                        Console.WriteLine((hint as string));

                        if (hint.Equals("ONLINE_CHANGED"))
                        {
                            IClient me = observable as IClient;

                            if (!me.online)
                            {
                                _sensorNames.Remove(me.name);
                            }
                        }
                    }));

                    //add a status listener
                    client.addStatusListener((name, bytes) =>
                    {
                        Console.WriteLine("{0}:status listener:{1}:{2}",
                            client.name,
                            name,
                            Encoding.UTF8.GetString(bytes, 0, bytes.Length));

                        _sensorDictionary[client.name].description = Encoding.UTF8.GetString(bytes, 0, bytes.Length);
                        _sensorDictionary[client.name].name = client.name;

                        add(client, Encoding.UTF8.GetString(bytes, 0, bytes.Length));
                    });
                }
            }
        }

        static async System.Threading.Tasks.Task Main(string[] args)
        {
            Console.WriteLine("hello cruel world");

            //ECOMMS Manager
            Manager _manager;

            //ADD INFLUX RECORDS TEST

            /*
            var cpuTime = new LineProtocolPoint(
                "working_set",
                new Dictionary<string, object>
                {
                    { "value", 77 },
                },
                new Dictionary<string, string>
                {
                    { "host", Environment.GetEnvironmentVariable("COMPUTERNAME") }
                },
                DateTime.UtcNow);

            var payload = new LineProtocolPayload();
            payload.Add(cpuTime);

            var client = new LineProtocolClient(new Uri("http://192.168.86.30:8086"), "firstdb");
            var influxResult = await client.WriteAsync(payload);
            if (!influxResult.Success)
                Console.Error.WriteLine(influxResult.ErrorMessage);
            */

            ////////////////////////

            //SETUP ECOMMS MANAGER AND START LISTENING TO CLIENT LIST CHANGES
            _manager = new Manager();

            //consider supporting nats list
            _manager.connect(@"nats://192.168.86.30:4222"); //.27 rPi, .30 maclinbook
            _manager.init();

            //addobserver(observerex) notifies with data which is the added client in this case
            _manager.addObserver(new ObserverAdapterEx((o, h, c) =>
            {
                //need to wait to notify until after base class has gotton response
                //to role request
                //or have library query first before creating client
                //WIP...

                var client = c as IClient;
                Thread.Sleep(3000);
                switch (h)
                {
                    case "CONNECTED":

                        if (client.role == Role.Sensor)
                        {

                            Console.WriteLine(client.name + " SENSOR CONNECTED");

                            addSensor(client);
                        }
                        break;
                }

            }));
        }
    }
}
