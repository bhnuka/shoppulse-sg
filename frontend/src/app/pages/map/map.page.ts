import { AfterViewInit, Component, OnDestroy } from '@angular/core';
import { CommonModule } from '@angular/common';
import * as L from 'leaflet';
import { ApiService, MapHotspot } from '../../core/api.service';
import { FilterBarComponent, FilterState } from '../../shared/filter-bar.component';

const DEFAULT_COORD = [1.3521, 103.8198];

@Component({
  selector: 'app-map-page',
  standalone: true,
  imports: [CommonModule, FilterBarComponent],
  templateUrl: './map.page.html',
  styleUrl: './map.page.css'
})
export class MapPage implements AfterViewInit, OnDestroy {
  private map?: L.Map;
  private geoLayer?: L.GeoJSON;
  filters: FilterState = {};
  loading = false;

  constructor(private readonly api: ApiService) {}

  ngAfterViewInit(): void {
    this.map = L.map('map', { zoomControl: true }).setView(DEFAULT_COORD as L.LatLngExpression, 11);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors'
    }).addTo(this.map);

    this.refresh();
  }

  onFiltersChange(next: FilterState): void {
    this.filters = next;
    this.refresh();
  }

  refresh(): void {
    if (!this.map) {
      return;
    }
    this.loading = true;
    this.api.getHotspots({ ssic: this.filters.ssic, from: this.filters.from, to: this.filters.to }).subscribe((data) => {
      this.renderGeo(data.hotspots);
      this.loading = false;
    });
  }

  private renderGeo(hotspots: MapHotspot[]): void {
    if (!this.map) {
      return;
    }

    if (this.geoLayer) {
      this.geoLayer.remove();
    }

    const maxCount = Math.max(...hotspots.map((h) => h.count), 1);
    const features = hotspots.map((h) => {
      const geometry = JSON.parse(h.geometry);
      return {
        type: 'Feature',
        properties: {
          name: h.name || h.subzone_id || 'Unknown',
          count: h.count,
          planning_area_id: h.planning_area_id || ''
        },
        geometry
      } as GeoJSON.Feature;
    });

    const geoJson = {
      type: 'FeatureCollection',
      features
    } as GeoJSON.FeatureCollection;

    this.geoLayer = L.geoJSON(geoJson, {
      style: (feature) => {
        const count = feature?.properties?.['count'] ?? 0;
        const intensity = count / maxCount;
        const color = `rgba(15, 118, 110, ${0.15 + intensity * 0.6})`;
        return {
          color: '#0f172a',
          weight: 1,
          fillColor: color,
          fillOpacity: 0.9
        };
      },
      onEachFeature: (feature, layer) => {
        const name = feature.properties?.['name'] ?? 'Unknown';
        const count = feature.properties?.['count'] ?? 0;
        layer.bindPopup(`${name}<br/>New entities: ${count}`);
      }
    });

    this.geoLayer.addTo(this.map);
  }

  ngOnDestroy(): void {
    if (this.map) {
      this.map.remove();
    }
  }
}
