// This file is part of Core WF which is licensed under the MIT license.
// See LICENSE file in the project root for full license information.

using System.Activities.DurableInstancing;
using System.Activities.Runtime.DurableInstancing;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Linq;

namespace JsonFileInstanceStore
{
    public class FileInstanceStore : InstanceStore
    {
        private readonly string _storeDirectoryPath;

        private readonly JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings
        {
            TypeNameHandling = TypeNameHandling.Auto,
            TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
            ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor,
            ObjectCreationHandling = ObjectCreationHandling.Replace,
            PreserveReferencesHandling = PreserveReferencesHandling.Objects
        };

        public FileInstanceStore(string storeDirectoryPath)
        {
            _storeDirectoryPath = storeDirectoryPath;
            Directory.CreateDirectory(storeDirectoryPath);
        }

        public bool KeepInstanceDataAfterCompletion
        {
            get;
            set;
        }

        private void DeleteFiles(Guid instanceId)
        {
            try
            {
                File.Delete(_storeDirectoryPath + "\\" + instanceId.ToString() + "-InstanceData");
                File.Delete(_storeDirectoryPath + "\\" + instanceId.ToString() + "-InstanceMetadata");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Caught exception trying to delete files for {0}: {1} - {2}", instanceId.ToString(), ex.GetType().ToString(), ex.Message);
            }
        }

        protected override IAsyncResult BeginTryCommand(InstancePersistenceContext context, InstancePersistenceCommand command, TimeSpan timeout, AsyncCallback callback, object state)
        {
            try
            {
                if (command is SaveWorkflowCommand)
                {
                    return new TypedCompletedAsyncResult<bool>(SaveWorkflow(context, (SaveWorkflowCommand)command), callback, state);
                }
                else if (command is LoadWorkflowCommand)
                {
                    return new TypedCompletedAsyncResult<bool>(LoadWorkflow(context, (LoadWorkflowCommand)command), callback, state);
                }
                else if (command is CreateWorkflowOwnerCommand)
                {
                    return new TypedCompletedAsyncResult<bool>(CreateWorkflowOwner(context, (CreateWorkflowOwnerCommand)command), callback, state);
                }
                else if (command is DeleteWorkflowOwnerCommand)
                {
                    return new TypedCompletedAsyncResult<bool>(DeleteWorkflowOwner(context, (DeleteWorkflowOwnerCommand)command), callback, state);
                }
                else if (command is TryLoadRunnableWorkflowCommand)
                {
                    return new TypedCompletedAsyncResult<bool>(TryLoadRunnableWorkflow(context, (TryLoadRunnableWorkflowCommand)command), callback, state);
                }
                return new TypedCompletedAsyncResult<bool>(false, callback, state);
            }
            catch (Exception e)
            {
                return new TypedCompletedAsyncResult<Exception>(e, callback, state);
            }
        }

        protected override bool EndTryCommand(IAsyncResult result)
        {
            if (result is TypedCompletedAsyncResult<Exception> exceptionResult)
            {
                throw exceptionResult.Data;
            }
            return TypedCompletedAsyncResult<bool>.End(result);
        }

        public static readonly XNamespace WorkflowNamespace = XNamespace.Get("urn:schemas-microsoft-com:System.Activities/4.0/properties");
        public static readonly XName PendingTimerExpirationPropertyName = WorkflowNamespace.GetName("TimerExpirationTime");

        private static DateTime? GetPendingTimerExpiration(SaveWorkflowCommand saveWorkflowCommand)
        {
            if (saveWorkflowCommand.InstanceData.TryGetValue(PendingTimerExpirationPropertyName, out var instanceValue))
                return ((DateTime)instanceValue.Value).ToUniversalTime();
            return default;
        }

        private bool SaveWorkflow(InstancePersistenceContext context, SaveWorkflowCommand command)
        {
            if (context.InstanceVersion == -1)
            {
                context.BindAcquiredLock(0);
            }

            if (command.CompleteInstance)
            {
                context.CompletedInstance();
                if (!KeepInstanceDataAfterCompletion)
                {
                    DeleteFiles(context.InstanceView.InstanceId);
                }
            }
            else
            {
                var timerTable = new Dictionary<Guid, DateTime>();

                var pendingTimerExpiration = GetPendingTimerExpiration(command);
                if (pendingTimerExpiration.HasValue)
                {
                    timerTable.Add(context.InstanceView.InstanceId, pendingTimerExpiration.Value);
                }

                Dictionary<string, InstanceValue> instanceData = SerializeablePropertyBagConvertXNameInstanceValue(command.InstanceData);
                Dictionary<string, InstanceValue> instanceMetadata = SerializeInstanceMetadataConvertXNameInstanceValue(context, command);

                try
                {
                    var serializedInstanceData = JsonConvert.SerializeObject(instanceData, Formatting.Indented, _jsonSerializerSettings);
                    File.WriteAllText(_storeDirectoryPath + "\\" + context.InstanceView.InstanceId + "-InstanceData", serializedInstanceData);

                    var serializedInstanceMetadata = JsonConvert.SerializeObject(instanceMetadata, Formatting.Indented, _jsonSerializerSettings);
                    File.WriteAllText(_storeDirectoryPath + "\\" + context.InstanceView.InstanceId + "-InstanceMetadata", serializedInstanceMetadata);

                    var serializedTimerTable = JsonConvert.SerializeObject(timerTable, Formatting.Indented, _jsonSerializerSettings);
                    File.WriteAllText(_storeDirectoryPath + "\\" + "TimerTable", serializedTimerTable);
                }
                catch (Exception)
                {
                    throw;
                }

                foreach (KeyValuePair<XName, InstanceValue> property in command.InstanceMetadataChanges)
                {
                    context.WroteInstanceMetadataValue(property.Key, property.Value);
                }

                context.PersistedInstance(command.InstanceData);
                if (command.CompleteInstance)
                {
                    context.CompletedInstance();
                }

                if (command.UnlockInstance || command.CompleteInstance)
                {
                    context.InstanceHandle.Free();
                }
            }

            return true;
        }

        private bool TryLoadRunnableWorkflow(InstancePersistenceContext context, TryLoadRunnableWorkflowCommand command)
        {
            var serializedInstanceData = File.ReadAllText(_storeDirectoryPath + "\\" + "TimerTable");
            var timerTable = JsonConvert.DeserializeObject<Dictionary<Guid, DateTime>>(serializedInstanceData, _jsonSerializerSettings);

            var expiredTimerInstances = timerTable
                .Where(kv =>
                {
                    var now = DateTime.UtcNow;
                    return kv.Value < now;
                })
                .Cast<KeyValuePair<Guid, DateTime>?>()
                .DefaultIfEmpty(null)
                .FirstOrDefault();

            if (!expiredTimerInstances.HasValue)
                return true;

            var instanceId = expiredTimerInstances.Value.Key;

            if (!context.InstanceView.IsBoundToInstance)
                context.BindInstance(instanceId);

            return LoadWorkflowInternal(context, command, instanceId);
        }

        private bool LoadWorkflow(InstancePersistenceContext context, LoadWorkflowCommand command)
        {
            if (command.AcceptUninitializedInstance)
            {
                return false;
            }

            var instanceId = context.InstanceView.InstanceId;
            return LoadWorkflowInternal(context, command, instanceId);
        }

        private bool LoadWorkflowInternal(InstancePersistenceContext context, InstancePersistenceCommand command, Guid instanceId)
        {
            if (context.InstanceVersion == -1)
            {
                context.BindAcquiredLock(0);
            }

            IDictionary<XName, InstanceValue> instanceData = null;
            IDictionary<XName, InstanceValue> instanceMetadata = null;

            Dictionary<string, InstanceValue> serializableInstanceData;
            Dictionary<string, InstanceValue> serializableInstanceMetadata;

            try
            {
                var serializedInstanceData = File.ReadAllText(_storeDirectoryPath + "\\" + instanceId + "-InstanceData");
                serializableInstanceData = JsonConvert.DeserializeObject<Dictionary<string, InstanceValue>>(serializedInstanceData, _jsonSerializerSettings);

                var serializedInstanceMetadata = File.ReadAllText(_storeDirectoryPath + "\\" + instanceId + "-InstanceMetadata");
                serializableInstanceMetadata = JsonConvert.DeserializeObject<Dictionary<string, InstanceValue>>(serializedInstanceMetadata, _jsonSerializerSettings);
            }
            catch (Exception)
            {
                throw;
            }

            instanceData = this.DeserializePropertyBagConvertXNameInstanceValue(serializableInstanceData);
            instanceMetadata = this.DeserializePropertyBagConvertXNameInstanceValue(serializableInstanceMetadata);

            context.LoadedInstance(InstanceState.Initialized, instanceData, instanceMetadata, null, null);

            return true;
        }

        private bool CreateWorkflowOwner(InstancePersistenceContext context, CreateWorkflowOwnerCommand command)
        {
            Guid instanceOwnerId = Guid.NewGuid();
            context.BindInstanceOwner(instanceOwnerId, instanceOwnerId);
            context.BindEvent(HasRunnableWorkflowEvent.Value);
            return true;
        }

        private bool DeleteWorkflowOwner(InstancePersistenceContext context, DeleteWorkflowOwnerCommand command)
        {
            return true;
        }

        private Dictionary<string, InstanceValue> SerializeablePropertyBagConvertXNameInstanceValue(IDictionary<XName, InstanceValue> source)
        {
            Dictionary<string, InstanceValue> scratch = new Dictionary<string, InstanceValue>();
            foreach (KeyValuePair<XName, InstanceValue> property in source)
            {
                bool writeOnly = (property.Value.Options & InstanceValueOptions.WriteOnly) != 0;

                if (!writeOnly && !property.Value.IsDeletedValue)
                {
                    scratch.Add(property.Key.ToString(), property.Value);
                }
            }

            return scratch;
        }

        private Dictionary<string, InstanceValue> SerializeInstanceMetadataConvertXNameInstanceValue(InstancePersistenceContext context, SaveWorkflowCommand command)
        {
            Dictionary<string, InstanceValue> metadata = null;

            foreach (var property in command.InstanceMetadataChanges)
            {
                if (!property.Value.Options.HasFlag(InstanceValueOptions.WriteOnly))
                {
                    if (metadata == null)
                    {
                        metadata = new Dictionary<string, InstanceValue>();
                        // copy current metadata. note that we must get rid of InstanceValue as it is not properly serializeable
                        foreach (var m in context.InstanceView.InstanceMetadata)
                        {
                            metadata.Add(m.Key.ToString(), m.Value);
                        }
                    }

                    if (metadata.ContainsKey(property.Key.ToString()))
                    {
                        if (property.Value.IsDeletedValue) metadata.Remove(property.Key.ToString());
                        else metadata[property.Key.ToString()] = property.Value;
                    }
                    else
                    {
                        if (!property.Value.IsDeletedValue) metadata.Add(property.Key.ToString(), property.Value);
                    }
                }
            }

            if (metadata == null)
                metadata = new Dictionary<string, InstanceValue>();

            return metadata;
        }

        private IDictionary<XName, InstanceValue> DeserializePropertyBagConvertXNameInstanceValue(Dictionary<string, InstanceValue> source)
        {
            Dictionary<XName, InstanceValue> destination = new Dictionary<XName, InstanceValue>();

            foreach (KeyValuePair<string, InstanceValue> property in source)
            {
                destination.Add(property.Key, property.Value);
            }

            return destination;
        }
    }
}
