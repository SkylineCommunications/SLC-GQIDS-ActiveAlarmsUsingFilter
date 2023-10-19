/*
****************************************************************************
*  Copyright (c) 2023,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

19/10/2023	1.0.0.1		Sebastiaan, Skyline	Initial version
****************************************************************************
*/

namespace GQIDSServiceAlarms_1
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Filters;
    using Skyline.DataMiner.Net.Helper;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Active alarms using filter")]
    public class ActiveAlarmsFilter : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch, IGQIInputArguments
    {
        private static readonly GQIStringArgument Filter = new GQIStringArgument("Filter") { IsRequired = true };

        private static readonly GQIStringColumn IDColumn = new GQIStringColumn("ID");
        private static readonly GQIStringColumn ElementColumn = new GQIStringColumn("Element");
        private static readonly GQIStringColumn ParameterColumn = new GQIStringColumn("Parameter");
        private static readonly GQIStringColumn ValueColumn = new GQIStringColumn("Value");
        private static readonly GQIDateTimeColumn TimeColumn = new GQIDateTimeColumn("Time");
        private static readonly GQIStringColumn SeverityColumn = new GQIStringColumn("Severity");
        private static readonly GQIStringColumn OwnerColumn = new GQIStringColumn("Owner");

        private GQIDMS _dms;
        private Task<List<AlarmEventMessage>> _alarms;
        private string _filter;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            _dms = args.DMS;
            return new OnInitOutputArgs();
        }

        public GQIArgument[] GetInputArguments()
        {
            return new GQIArgument[]
            {
                Filter,
            };
        }

        public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
        {
            args.TryGetArgumentValue(Filter, out _filter);
            return new OnArgumentsProcessedOutputArgs();
        }

        public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
        {
            if (string.IsNullOrWhiteSpace(_filter))
                return new OnPrepareFetchOutputArgs();

            _alarms = Task.Factory.StartNew(() =>
            {
                var filterItem = new AlarmFilterItemFilter(new[] { $"{_filter} (shared filter)" });
                var filter = new AlarmFilter(filterItem);
                var msg = new GetActiveAlarmsMessage() { Filter = filter };

                var alarmsResponse = _dms.SendMessage(msg) as ActiveAlarmsResponseMessage;
                if (alarmsResponse != null)
                {
                    return alarmsResponse.ActiveAlarms.WhereNotNull().ToList();
                }

                return null;
            });
            return new OnPrepareFetchOutputArgs();
        }

        public GQIColumn[] GetColumns()
        {
            return new GQIColumn[]
            {
                IDColumn,
                ElementColumn,
                ParameterColumn,
                ValueColumn,
                TimeColumn,
                SeverityColumn,
                OwnerColumn,
            };
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            if (_alarms == null)
                return new GQIPage(new GQIRow[0]);

            _alarms.Wait();

            var alarms = _alarms.Result;
            if (alarms == null)
                throw new GenIfException("No alarms found.");

            if (alarms.Count == 0)
                return new GQIPage(new GQIRow[0]);

            var rows = new List<GQIRow>(alarms.Count);

            foreach (var alarm in alarms)
            {
                var cells = new[]
                {
                    new GQICell {Value= $"{alarm.DataMinerID}/{alarm.AlarmID }"}, // IDColumn
                    new GQICell {Value= alarm.ElementName }, // ElementColumn,
                    new GQICell {Value= alarm.ParameterName }, // ParameterColumn,
                    new GQICell {Value= alarm.DisplayValue }, // ValueColumn,
                    new GQICell {Value= alarm.CreationTime.ToUniversalTime() }, // TimeColumn,
                    new GQICell {Value= alarm.Severity }, // SeverityColumn,
                    new GQICell {Value = alarm.Owner}, // OwnerColumn
                };

                rows.Add(new GQIRow(cells));
            }

            return new GQIPage(rows.ToArray()) { HasNextPage = false };
        }
    }
}