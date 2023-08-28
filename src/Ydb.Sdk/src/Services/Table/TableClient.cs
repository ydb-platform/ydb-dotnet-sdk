﻿using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
    public class TableClientConfig
    {
        public SessionPoolConfig SessionPoolConfig { get; }

        public TableClientConfig(
            SessionPoolConfig? sessionPoolConfig = null)
        {
            SessionPoolConfig = sessionPoolConfig ?? new SessionPoolConfig();
        }
    }

    public partial class TableClient : ClientBase, IDisposable
    {
        private ISessionPool _sessionPool;
        private bool _disposed = false;

        public TableClient(Driver driver, TableClientConfig? config = null)
            : base(driver)
        {
            config ??= new TableClientConfig();

            _sessionPool = new SessionPool(
                driver: driver,
                config: config.SessionPoolConfig);
        }

        internal TableClient(Driver driver, ISessionPool sessionPool)
            : base(driver)
        {
            _sessionPool = sessionPool;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _sessionPool.Dispose();
            }

            _disposed = true;
        }
    }
}
