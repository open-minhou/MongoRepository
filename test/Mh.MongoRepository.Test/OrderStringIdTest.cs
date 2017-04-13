using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;
using Mh.MongoRepository.TestInstructure;

namespace Mh.MongoRepository.Test
{
    [TestClass]
    public class OrderStringIdTest
    {
        static OrderStringIdRepository _repository;
        static OrderStringId _order;
        static OrderStringIdTest()
        {
            _repository = new OrderStringIdRepository();
            _order = new OrderStringId { ID=Guid.NewGuid().ToString("N"),Name="testName"};
        }
        [TestMethod]
        public async Task InsertAndGetTest()
        {
            await _repository.InsertAsync(_order);
        }
    }
}
