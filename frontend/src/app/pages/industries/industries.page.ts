import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ApiService, RankingItem } from '../../core/api.service';
import { FilterBarComponent, FilterState } from '../../shared/filter-bar.component';

@Component({
  selector: 'app-industries-page',
  standalone: true,
  imports: [CommonModule, FilterBarComponent],
  templateUrl: './industries.page.html',
  styleUrl: './industries.page.css'
})
export class IndustriesPage {
  filters: FilterState = {};
  rankings: RankingItem[] = [];
  selected?: RankingItem;

  constructor(private readonly api: ApiService) {
    this.refresh();
  }

  onFiltersChange(next: FilterState): void {
    this.filters = next;
    this.refresh();
  }

  refresh(): void {
    this.api.getTopSsic({
      from: this.filters.from,
      to: this.filters.to,
      area: this.filters.area,
      limit: 15
    }).subscribe((data) => {
      this.rankings = data.items;
      this.selected = data.items[0];
    });
  }

  select(item: RankingItem): void {
    this.selected = item;
  }
}
