FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["Lesson1/Lesson1.csproj", "Lesson1/"]
RUN dotnet restore "Lesson1/Lesson1.csproj"
COPY . .
WORKDIR "/src/Lesson1"
RUN dotnet build "Lesson1.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Lesson1.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Lesson1.dll"]
