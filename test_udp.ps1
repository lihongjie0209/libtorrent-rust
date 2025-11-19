# Test UDP connectivity to DHT bootstrap nodes
$bootstrapNodes = @(
    @{Host="router.bittorrent.com"; Port=6881},
    @{Host="dht.transmissionbt.com"; Port=6881},
    @{Host="router.utorrent.com"; Port=6881},
    @{Host="dht.libtorrent.org"; Port=25401},
    @{Host="router.silotis.us"; Port=6881},
    @{Host="dht.aelitis.com"; Port=6881},
    @{Host="router.bitcomet.com"; Port=6881}
)

Write-Host "Testing DNS resolution and UDP connectivity..." -ForegroundColor Green
Write-Host ""

foreach ($node in $bootstrapNodes) {
    $hostname = $node.Host
    $port = $node.Port
    
    Write-Host "Testing $hostname`:$port" -ForegroundColor Yellow
    
    # Test DNS resolution
    try {
        $ips = [System.Net.Dns]::GetHostAddresses($hostname) | Where-Object { $_.AddressFamily -eq 'InterNetwork' }
        if ($ips.Count -gt 0) {
            Write-Host "  ✓ Resolved to: $($ips -join ', ')" -ForegroundColor Green
            
            # Try to create UDP client on random port
            $udpClient = New-Object System.Net.Sockets.UdpClient
            $udpClient.Client.ReceiveTimeout = 1000
            Write-Host "  ✓ UDP socket created on port $($udpClient.Client.LocalEndPoint.Port)" -ForegroundColor Green
            $udpClient.Close()
        } else {
            Write-Host "  ✗ No IPv4 addresses found" -ForegroundColor Red
        }
    }
    catch {
        Write-Host "  ✗ Error: $($_.Exception.Message)" -ForegroundColor Red
    }
    Write-Host ""
}

Write-Host ""
Write-Host "Checking Windows Firewall status..." -ForegroundColor Green
try {
    $firewallProfiles = Get-NetFirewallProfile
    foreach ($profile in $firewallProfiles) {
        Write-Host "$($profile.Name): Enabled=$($profile.Enabled)" -ForegroundColor $(if ($profile.Enabled) { "Yellow" } else { "Green" })
    }
}
catch {
    Write-Host "Unable to check firewall status: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "Testing port binding on 40000-40010..." -ForegroundColor Green
for ($port = 40000; $port -lt 40010; $port++) {
    try {
        $udp = New-Object System.Net.Sockets.UdpClient $port
        Write-Host "  ✓ Port $port is available" -ForegroundColor Green
        $udp.Close()
        break
    }
    catch {
        Write-Host "  ✗ Port $port failed: $($_.Exception.Message)" -ForegroundColor Red
    }
}
