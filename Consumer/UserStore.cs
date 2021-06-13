using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

internal class UserStore
{
    private readonly string _folderPath;

    public UserStore(string folderPath)
    {
        _folderPath = folderPath;
        Directory.CreateDirectory(_folderPath); // make sure directory exists
    }

    public async Task<UserBalance?> GetUserBalanceAsync(long userId)
    {
        var userFilePath = GetUserFilePath(userId);
        if (File.Exists(userFilePath))
        {
            var fileLines = await File.ReadAllLinesAsync(userFilePath);
            var lastUserBalance = fileLines.Last(l => !string.IsNullOrWhiteSpace(l));
            var userBalance = JsonSerializer.Deserialize<UserBalance>(lastUserBalance);
            return userBalance;
        }
        else
        {
            return null;
        }
    }

    public async Task SaveUserBalanceAsync(long userId, UserBalance userBalance)
    {
        var userFilePath = GetUserFilePath(userId);
        var userBalanceLine = JsonSerializer.Serialize(userBalance);
        await File.AppendAllLinesAsync(userFilePath, new[] {userBalanceLine});
    }

    private string GetUserFilePath(long userId)
    {
        return Path.ChangeExtension(Path.Combine(_folderPath, userId.ToString()), ".json");
    }
}