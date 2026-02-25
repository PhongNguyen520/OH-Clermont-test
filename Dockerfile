FROM mcr.microsoft.com/playwright/dotnet:v1.48.0-jammy AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["OH_Clermont/OH_Clermont.csproj", "OH_Clermont/"]
RUN dotnet restore "OH_Clermont/OH_Clermont.csproj"
COPY . .
WORKDIR "/src/OH_Clermont"
RUN dotnet build "OH_Clermont.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "OH_Clermont.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OH_Clermont.dll"]
