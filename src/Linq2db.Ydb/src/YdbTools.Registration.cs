using System.Runtime.CompilerServices;
using LinqToDB.Data;

namespace LinqToDB.Internal.DataProvider.Ydb
{
    public static partial class YdbTools // сделайте YdbTools partial, либо поместите код в сам файл YdbTools.cs
    {
        [ModuleInitializer]
        public static void Register()
        {
            // ЭТО главный хук: связываем имя "YDB" и ваш провайдер
            DataConnection.AddProviderDetector(ProviderDetector);
        }
    }
}
