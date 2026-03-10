FROM mcr.microsoft.com/playwright/dotnet:v1.48.0-jammy AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["CountyFusion/CountyFusion.csproj", "CountyFusion/"]
RUN dotnet restore "CountyFusion/CountyFusion.csproj"
COPY . .
WORKDIR "/src/CountyFusion"
RUN dotnet build "CountyFusion.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "CountyFusion.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "CountyFusion.dll"]
