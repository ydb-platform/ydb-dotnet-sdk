using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public class SemaphoreDescriptionClient
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }
    private readonly List<Session> _ownersList;
    private readonly List<Session> _waitersList;

    public SemaphoreDescriptionClient(SemaphoreDescription description)
    {
        Name = description.Name;
        Data = description.Data.ToByteArray();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;
        _ownersList = new List<Session>();
        _waitersList = new List<Session>();

        foreach (var owner in description.Owners)
        {
            _ownersList.Add(new Session(owner));
        }

        foreach (var waiters in description.Waiters)
        {
            _waitersList.Add(new Session(waiters));
        }
    }

    public List<Session> GetOwnersList() => _ownersList;
    public List<Session> GetWaitersList() => _waitersList;

    public class Session
    {
        public ulong Id { get; }
        public ulong TimeoutMillis { get; }
        public ulong Count { get; }
        public byte[] Data { get; }
        public ulong OrderId { get; }

        public Session(SemaphoreSession semaphoreSession)
        {
            Id = semaphoreSession.SessionId;
            TimeoutMillis = semaphoreSession.TimeoutMillis;
            Count = semaphoreSession.Count;
            Data = semaphoreSession.Data.ToByteArray();
            OrderId = semaphoreSession.OrderId;
        }
    }
}

/*
 * using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

public class SemaphoreDescriptionClient
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }

    private readonly IReadOnlyList<Session> _owners;
    private readonly IReadOnlyList<Session> _waiters;

    public SemaphoreDescriptionClient(SemaphoreDescription description)
    {
        if (description == null)
            throw new ArgumentNullException(nameof(description));

        Name = description.Name;
        Data = description.Data?.ToByteArray() ?? Array.Empty<byte>();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;

        _owners = description.Owners?
            .Select(o => new Session(o))
            .ToList()
            .AsReadOnly()
            ?? new List<Session>().AsReadOnly();

        _waiters = description.Waiters?
            .Select(w => new Session(w))
            .ToList()
            .AsReadOnly()
            ?? new List<Session>().AsReadOnly();
    }

    public IReadOnlyList<Session> Owners => _owners;
    public IReadOnlyList<Session> Waiters => _waiters;

    public class Session
    {
        public ulong Id { get; }
        public ulong TimeoutMillis { get; }
        public ulong Count { get; }
        public byte[] Data { get; }
        public ulong OrderId { get; }

        public Session(SemaphoreSession semaphoreSession)
        {
            if (semaphoreSession == null)
                throw new ArgumentNullException(nameof(semaphoreSession));

            Id = semaphoreSession.SessionId;
            TimeoutMillis = semaphoreSession.TimeoutMillis;
            Count = semaphoreSession.Count;
            Data = semaphoreSession.Data?.ToByteArray() ?? Array.Empty<byte>();
            OrderId = semaphoreSession.OrderId;
        }
    }
}
 */

/*
У тебя в целом всё ок по логике, но есть несколько проблем, которые в C# лучше поправить — сейчас код слишком «сырой» с точки зрения инкапсуляции и безопасности данных.

Разберу и сразу дам исправленную версию.

---

# ❗ Что стоит исправить

## 1. Возврат `List<>` наружу

```csharp
public List<Session> GetOwnersList() => _ownersList;
```

Ты отдаёшь **живую коллекцию**, и внешний код может её менять → это ломает инварианты.

👉 Нужно отдавать `IReadOnlyList<>`

---

## 2. `byte[] Data` — мутируемый

```csharp
public byte[] Data { get; }
```

Массив можно изменить снаружи → объект перестаёт быть immutable.

---

## 3. Нет проверок на `null`

```csharp
public SemaphoreDescriptionClient(SemaphoreDescription description)
```

Если `description == null` → будет `NullReferenceException`

---

## 4. Лишние копии и стиль

```csharp
foreach (var waiters in description.Waiters)
```

→ переменная названа во множественном числе, но это один элемент

---

## 5. Лучше сделать класс иммутабельным

Сейчас он почти immutable — доведём до конца

---

# ✅ Исправленная версия

```csharp
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

public class SemaphoreDescriptionClient
{
    public string Name { get; }
    public byte[] Data { get; }
    public ulong Count { get; }
    public ulong Limit { get; }
    public bool Ephemeral { get; }

    private readonly IReadOnlyList<Session> _owners;
    private readonly IReadOnlyList<Session> _waiters;

    public SemaphoreDescriptionClient(SemaphoreDescription description)
    {
        if (description == null)
            throw new ArgumentNullException(nameof(description));

        Name = description.Name;
        Data = description.Data?.ToByteArray() ?? Array.Empty<byte>();
        Count = description.Count;
        Limit = description.Limit;
        Ephemeral = description.Ephemeral;

        _owners = description.Owners?
            .Select(o => new Session(o))
            .ToList()
            .AsReadOnly()
            ?? new List<Session>().AsReadOnly();

        _waiters = description.Waiters?
            .Select(w => new Session(w))
            .ToList()
            .AsReadOnly()
            ?? new List<Session>().AsReadOnly();
    }

    public IReadOnlyList<Session> Owners => _owners;
    public IReadOnlyList<Session> Waiters => _waiters;

    public class Session
    {
        public ulong Id { get; }
        public ulong TimeoutMillis { get; }
        public ulong Count { get; }
        public byte[] Data { get; }
        public ulong OrderId { get; }

        public Session(SemaphoreSession semaphoreSession)
        {
            if (semaphoreSession == null)
                throw new ArgumentNullException(nameof(semaphoreSession));

            Id = semaphoreSession.SessionId;
            TimeoutMillis = semaphoreSession.TimeoutMillis;
            Count = semaphoreSession.Count;
            Data = semaphoreSession.Data?.ToByteArray() ?? Array.Empty<byte>();
            OrderId = semaphoreSession.OrderId;
        }
    }
}
```

---

# 🔥 Что улучшилось

### ✔ Безопасность

* нет утечки mutable коллекций
* нет `null`-падений

### ✔ Иммутабельность

* объект теперь read-only снаружи

### ✔ API стал чище

```csharp
description.Owners
description.Waiters
```

вместо:

```csharp
GetOwnersList()
```

---

# ⚠️ Если хочешь ещё лучше (рекомендую)

Можно сделать прям «идеально» под .NET:

### 1. Использовать `record`

### 2. Заменить `byte[]` на `ReadOnlyMemory<byte>`

---

## 💎 Версия уровня production

```csharp
public record SemaphoreSessionClient(
    ulong Id,
    ulong TimeoutMillis,
    ulong Count,
    ReadOnlyMemory<byte> Data,
    ulong OrderId
);
```

---

# 👉 Вывод

Ты уже сделал почти правильную модель 👍
Я просто:

* закрыл мутабельность
* убрал утечки
* привёл к .NET-стилю

---

Если хочешь — могу дальше:

* сделать это в виде **DTO + Mapper (AutoMapper / manual)**
* или оптимизировать под **high-load (без лишних аллокаций)**
*/