using System;
using System.Activities;
using System.Activities.DurableInstancing;
using System.Threading;
using Test.Common.TestObjects.Activities;
using Test.Common.TestObjects.Runtime;
using Xunit;

namespace TestCases.Activities
{
    public class Delay : IDisposable
    {
        /// <summary>
        /// DoWhile is persisted in the middle of execution
        /// </summary>        
        [Fact]
        public void DelayPersisted()
        {
            Variable<int> counter = new Variable<int>("Counter", 0);
            TestBlockingActivity blocking = new TestBlockingActivity("B");

            TestSequence seq = new TestSequence
            {
                Activities =
                {
                    new TestWriteLine("Before") {Message = "Before"},
                    new TestDelay()
                    {
                        Duration = TimeSpan.FromSeconds(1)
                    },
                    new TestWriteLine("After") {Message = "After"}
                }
            };

            TestSequence seq2 = new TestSequence
            {
                Activities =
                {
                    new TestWriteLine("Before") {Message = "Before"},
                    new TestDelay()
                    {
                        Duration = TimeSpan.FromSeconds(1)
                    },
                    new TestWriteLine("After") {Message = "After"}
                }
            };


            JsonFileInstanceStore.FileInstanceStore jsonStore = new JsonFileInstanceStore.FileInstanceStore(".\\~");

            var instanceHandle = jsonStore.CreateInstanceHandle();
            var instanceView = jsonStore.Execute(instanceHandle, new CreateWorkflowOwnerCommand(), TimeSpan.MaxValue);
            var instanceOwner = instanceView.InstanceOwner;
            jsonStore.DefaultInstanceOwner = instanceOwner;

            //Guid id;
            using (TestWorkflowRuntime runtime =
                TestRuntime.CreateTestWorkflowRuntime(seq, null, jsonStore, PersistableIdleAction.Unload))
            {
                runtime.ExecuteWorkflow();

                runtime.WaitForUnloaded();

                //id = runtime.CurrentWorkflowInstanceId;

                Thread.Sleep(TimeSpan.FromSeconds(2));
               
                //runtime.InitiateWorkflowForLoad();
                //runtime.LoadRunnableInitiatedWorkflowForInstance(/*TimeSpan.Zero, id*/);
                runtime.LoadRunnableWorkflow();

                runtime.ResumeWorkflow();

                runtime.WaitForCompletion();


            }


        }

        public void Dispose()
        {

        }
    }
}