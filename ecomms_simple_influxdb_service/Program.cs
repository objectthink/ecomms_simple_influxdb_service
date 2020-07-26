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
using System.Linq;
using ECOMMS_Participant;
using NATS.Client;
using InfluxDB.Client;

namespace ecomms_simple_influxdb_service
{
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

    public class SensorDataPoint
    {
        public int temperature { get; set; }
        public int humidity { get; set; }
        public float level { get; set; }
    }

    public class InstrumentStatus
    {
        public string StatusType { get; set; }
    }

    public class InstrumentDataStatus
    {
        public string StatusType { get; set; }
        public InstrumentFloatData FloatData { get; set; }
        public int PtIndex { get; set; }
    }

    public class InstrumentFloatData
    {
        public float AlphaData { get; set; }
        public float DeltaLengthData { get; set; }
        public float DynComplMagData { get; set; }
        public float DynComplPhaseData { get; set; }
        public float DynNormalForce { get; set; }
        public float DynNormalMagData { get; set; }
        public float DynNormalPhaseData { get; set; }
        public float DynRateData { get; set; }
        public float DynStrainCmdData { get; set; }
        public float DynStrainMagData { get; set; }
        public float DynStrainPhaseData { get; set; }
        public float DynTorqueMagData { get; set; }
        public float DynTorquePhaseData { get; set; }
        public float SampleGapData { get; set; }
        public float ScaleNormWaveData { get; set; }
        public float ScaleStrnWaveData { get; set; }
        public float ScaleTorqWaveData { get; set; }
        public float StaticTensionZData { get; set; }
        public float StdyTorqueMagData { get; set; }
        public float SteadyRateData { get; set; }
        public float TempData { get; set; }
        public float TimeData { get; set; }
    }

    public class InstrumentRealtimeStatus
    {
        public string StatusType { get; set; }
        public InstrumentSignal[] Signals { get; set; }
    }

    public class InstrumentSignal
    {
        public string Name { get; set; }
        public string Units { get; set; }
        public string Value { get; set; }
    }

    public class PersistentStoreService : ServiceParticipant
    {
        List<string> _sensorNames = new List<string>();
        List<SensorData> _sensorDataList = new List<SensorData>();
        Dictionary<string, SensorData> _sensorDictionary = new Dictionary<string, SensorData>();

        public PersistentStoreService(
            string id) : base(id, ECOMMS_Entity.Type.PersistentStore)
        {
        }

        public async override void get(string what, Msg message)
        {
            //influxdb query example
            char[] Token = "".ToCharArray();
            var influxDBClient = InfluxDBClientFactory.Create("http://192.168.86.27:8086", Token);

            switch (what)
            {
                case "sensorsNamesList":
                    //influxdb query

                    //create a string from the last minutes worth of data in all tables
                    string data = "";
                    var flux = "from(bucket:\"first\") |> range(start: -1m)";

                    var fluxTables = await influxDBClient.GetQueryApi().QueryAsync(flux, "sensors");
                    fluxTables.ForEach(fluxTable =>
                    {
                        var fluxRecords = fluxTable.Records;
                        fluxRecords.ForEach(fluxRecord =>
                        {
                            //Console.WriteLine($"{fluxRecord.GetTime()}: {fluxRecord.GetValue()}");
                            data += $"{fluxRecord.GetTime()}: {fluxRecord.GetValue()}";
                        });
                    });

                    influxDBClient.Dispose();                    ////////////////

                    //send reply
                    replyTo(message, data);
                    break;
                default:
                    //not handled
                    base.get(what, message);
                    break;
            }
        }

        /// <summary>
        /// add sensor point
        /// </summary>
        /// <param name="client"></param>
        /// <param name="status"></param>
        private async void add(IClient client, string status)
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

            var influx = new LineProtocolClient(new Uri("http://192.168.86.27:8086"), "first");
            var influxResult = await influx.WriteAsync(payload);

            if (!influxResult.Success)
                Console.Error.WriteLine(influxResult.ErrorMessage);
        }

        //add a sensor to our list of sensors
        private void addSensor(IClient client)
        {
            if (client.role == Role.Sensor)
            {

                Console.WriteLine(client.name + " SENSOR ADDED");

                if (!_sensorNames.Contains(client.id))
                {
                    _sensorNames.Add(client.id);
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
                                _sensorNames.Remove(me.id);
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

        private void addInstrument(IClient client)
        {
            if (client.role == Role.Instrument)
            {
                Console.WriteLine(" INSTRUMENT CONNECTED {0}", client.name);

                //add a status listener
                client.addStatusListener((name, bytes) =>
                {
                    //Console.WriteLine("{0}:status listener:{1}:{2}",
                    //    client.name,
                    //    name,
                    //    Encoding.UTF8.GetString(bytes, 0, bytes.Length));

                    addInstrumentDataPoint(client, Encoding.UTF8.GetString(bytes, 0, bytes.Length));
                });
            }
        }

        private async void addInstrumentDataPoint(IClient client, string status)
        {
            InstrumentStatus sdp = JsonSerializer.Deserialize<InstrumentStatus>(status);

            Console.WriteLine(sdp.StatusType);

            //do something with data status
            if (sdp.StatusType.Equals("Data"))
            {
                InstrumentDataStatus dataStatus = JsonSerializer.Deserialize<InstrumentDataStatus>(status);

                Console.WriteLine(status);

                var point = new LineProtocolPoint(
                    "experiments",
                    new Dictionary<string, object>
                    {
                        { "AlphaData"       , dataStatus.FloatData.AlphaData },
                        { "DeltaLengthData" , dataStatus.FloatData.DeltaLengthData },
                        { "DynComplMagData"       , dataStatus.FloatData.DynComplMagData },
                        { "DynComplPhaseData"       , dataStatus.FloatData.DynComplPhaseData },
                        { "DynNormalForce"       , dataStatus.FloatData.DynNormalForce },
                        { "DynNormalMagData"       , dataStatus.FloatData.DynNormalMagData },
                        { "DynNormalPhaseData"       , dataStatus.FloatData.DynNormalPhaseData },
                        { "DynRateData"       , dataStatus.FloatData.DynRateData },
                        { "DynStrainCmdData"       , dataStatus.FloatData.DynStrainCmdData },
                        { "DynStrainMagData"       , dataStatus.FloatData.DynStrainMagData },
                        { "DynStrainPhaseData"       , dataStatus.FloatData.DynStrainPhaseData },
                        { "DynTorqueMagData"       , dataStatus.FloatData.DynTorqueMagData },
                        { "DynTorquePhaseData"       , dataStatus.FloatData.DynTorquePhaseData },
                        { "SampleGapData"       , dataStatus.FloatData.SampleGapData },
                        { "ScaleNormWaveData"       , dataStatus.FloatData.ScaleNormWaveData },
                        { "ScaleStrnWaveData"       , dataStatus.FloatData.ScaleStrnWaveData },
                        { "ScaleTorqWaveData"       , dataStatus.FloatData.ScaleTorqWaveData },
                        { "StaticTensionZData"       , dataStatus.FloatData.StaticTensionZData },
                        { "StdyTorqueMagData"       , dataStatus.FloatData.StdyTorqueMagData },
                        { "SteadyRateData"       , dataStatus.FloatData.SteadyRateData },
                        { "TempData"       , dataStatus.FloatData.TempData },
                        { "TimeData"       , dataStatus.FloatData.TimeData },
                    },

                    new Dictionary<string, string>
                    {
                        { "instrument", client.name },
                        { "experiment", "ID GOES HERE!" }
                    },

                    DateTime.UtcNow);

                var payload = new LineProtocolPayload();
                payload.Add(point);

                var influx = new LineProtocolClient(new Uri("http://192.168.86.27:8086"), "first");
                var influxResult = await influx.WriteAsync(payload);

                if (!influxResult.Success)
                    Console.Error.WriteLine(influxResult.ErrorMessage);
            }

            if (sdp.StatusType.Equals("RealTime"))
            {
                Dictionary<string, object> signals = new Dictionary<string, object>();

                InstrumentRealtimeStatus dataStatus = JsonSerializer.Deserialize<InstrumentRealtimeStatus>(status);

                Console.WriteLine(status);
                foreach (InstrumentSignal signal in dataStatus.Signals)
                {
                    Console.WriteLine("{0} {1} {2}",
                        signal.Name,
                        signal.Value,
                        signal.Units);

                    if (!signals.Keys.Contains(signal.Name))
                        signals.Add(signal.Name, signal.Value);
                }

                var point = new LineProtocolPoint(
                    "instruments",
                    signals,

                    new Dictionary<string, string>
                    {
                        { "instrument", client.name }
                    },

                    DateTime.UtcNow);

                var payload = new LineProtocolPayload();
                payload.Add(point);

                var influx = new LineProtocolClient(new Uri("http://192.168.86.27:8086"), "first");
                var influxResult = await influx.WriteAsync(payload);

                if (!influxResult.Success)
                    Console.Error.WriteLine(influxResult.ErrorMessage);

            }
        }

        public override void init()
        {
            base.init();

            //ECOMMS Manager
            Manager _manager;

            ////////////////////////

            //SETUP ECOMMS MANAGER AND START LISTENING TO CLIENT LIST CHANGES
            _manager = new Manager();

            //consider supporting nats list
            _manager.connect(@"nats://192.168.86.31:7222"); //.27 rPi, .30 maclinbook
            _manager.init();

            //addobserver(observerex) notifies with data which is the added client in this case
            _manager.addObserver(new ObserverAdapterEx((o, h, c) =>
            {
                //need to wait to notify until after base class has gotton response
                //to role request
                //or have library query first before creating client
                //WIP...

                var client = c as IClient;

                switch (h)
                {
                    case "CONNECTED":

                        if (client.role == Role.Sensor)
                        {

                            Console.WriteLine(client.name + " SENSOR CONNECTED");

                            addSensor(client);
                        }

                        if (client.role == Role.Instrument)
                        {
                            addInstrument(client);
                        }
                        break;
                }

            }));
        }
    }

    class Program
    {
        static int Main(string[] args)
        {
            Console.WriteLine("hello cruel world");

            //unique id
            var id = Guid.NewGuid().ToString();

            //create persistent store service instance
            PersistentStoreService service = new PersistentStoreService(id);
            service.connect(@"nats://192.168.86.31:7222");
            service.init();

            //TEST PESISTENT STORE GET REQUEST
            //CLIENT WOULD BE ACQUIRED IN CLIENT APPLICATION USING MANAGER
            var serviceClient = new Client(id, Role.Service, ECOMMS_Entity.Type.PersistentStore);
            serviceClient.connect(@"nats://192.168.86.31:7222");
            serviceClient.init();

            //test get
            serviceClient.doGet("sensorsNamesList", (s) =>
            {
                Console.WriteLine("persistent store service returned:" + s);
            });

            //////////////////////////////////

            return 0;
        }
    }
}
