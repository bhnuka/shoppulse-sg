import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService, OverviewResponse, RankingsResponse, TrendsResponse } from '../../core/api.service';
import { FilterBarComponent, FilterState } from '../../shared/filter-bar.component';
import { LineChartComponent } from '../../shared/line-chart.component';

@Component({
  selector: 'app-overview-page',
  standalone: true,
  imports: [CommonModule, FilterBarComponent, LineChartComponent],
  templateUrl: './overview.page.html',
  styleUrl: './overview.page.css'
})
export class OverviewPage {
  filters: FilterState = {};
  overview?: OverviewResponse;
  trends?: TrendsResponse;
  rankings?: RankingsResponse;
  loading = false;

  constructor(private readonly api: ApiService) {
    this.refresh();
  }

  onFiltersChange(next: FilterState): void {
    this.filters = next;
    this.refresh();
  }

  refresh(): void {
    this.loading = true;
    const params = {
      from: this.filters.from,
      to: this.filters.to,
      ssic: this.filters.ssic,
      area: this.filters.area
    };

    this.api.getOverview(params).subscribe((data) => (this.overview = data));
    this.api.getTrends(params).subscribe((data) => {
      this.trends = data;
      this.loading = false;
    });
    this.api.getTopSsic({ from: this.filters.from, to: this.filters.to, area: this.filters.area, limit: 10 })
      .subscribe((data) => (this.rankings = data));
  }

  get chartLabels(): string[] {
    return this.trends?.series.map((point) => String(point.month)) ?? [];
  }

  get chartValues(): number[] {
    return this.trends?.series.map((point) => point.count) ?? [];
  }
}
