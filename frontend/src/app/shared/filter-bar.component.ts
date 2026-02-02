import { Component, EventEmitter, Input, Output, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService, SsicCategory } from '../core/api.service';

export interface FilterState {
  from?: string;
  to?: string;
  ssic?: string;
  ssicCategory?: string;
  area?: string;
}

@Component({
  selector: 'app-filter-bar',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './filter-bar.component.html',
  styleUrl: './filter-bar.component.css'
})
export class FilterBarComponent implements OnInit {
  constructor(private readonly api: ApiService) {}

  @Input() state: FilterState = {};
  @Output() stateChange = new EventEmitter<FilterState>();

  categories: SsicCategory[] = [];
  filteredCategories: (SsicCategory & { display: string })[] = [];
  categoryInput = '';
  sectorLabel: Record<string, string> = {};
  groupedCategories: { sector: string; label: string; items: (SsicCategory & { display: string })[] }[] = [];

  showCategoryList = false;


  ngOnInit(): void {
    this.api.getSsicCategories().subscribe((data) => {
      this.categories = data.categories ?? [];
      const sectors = data.sectors ?? [];
      this.sectorLabel = sectors.reduce((acc, s) => {
        acc[s.id] = s.label;
        return acc;
      }, {} as Record<string, string>);

      this.filteredCategories = this.categories.map((c) => ({
        ...c,
        display: this.formatDisplay(c)
      }));

      this.updateGroupedCategories('');
    });
  }

  private formatDisplay(category: SsicCategory): string {
    return category.label;
  }

  private updateGroupedCategories(filter: string): void {
    const query = filter.trim().toLowerCase();
    const items = query
      ? this.filteredCategories.filter((c) => {
          const labelMatch = c.label.toLowerCase().includes(query);
          const keywordMatch = (c.keywords || []).some((k) => k.toLowerCase().includes(query));
          return labelMatch || keywordMatch;
        })
      : this.filteredCategories;

    const grouped: Record<string, (SsicCategory & { display: string })[]> = {};
    items.forEach((c) => {
      const key = c.sector || 'other';
      if (!grouped[key]) {
        grouped[key] = [];
      }
      grouped[key].push(c);
    });

    this.groupedCategories = Object.keys(grouped).map((key) => ({
      sector: key,
      label: this.sectorLabel[key] || key,
      items: grouped[key]
    }));
  }

  selectCategory(category: SsicCategory & { display: string }): void {
    this.categoryInput = category.label;
    this.state.ssicCategory = category.id;
    this.state.ssic = undefined;
    this.showCategoryList = false;
    this.onChange();
  }

  showList(): void {
    this.showCategoryList = true;
  }

  hideList(): void {
    setTimeout(() => {
      this.showCategoryList = false;
    }, 150);
  }

  onCategoryInput(): void {
    const value = (this.categoryInput || '').trim();
    this.updateGroupedCategories(value);
    this.showCategoryList = true;
    if (!value) {
      this.state.ssicCategory = undefined;
      this.state.ssic = undefined;
      this.onChange();
      return;
    }
    const lower = value.toLowerCase();
    const direct = this.filteredCategories.find((c) => c.label.toLowerCase() == lower);
    const keyword = this.filteredCategories.find((c) => (c.keywords || []).some((k) => k.toLowerCase() == lower));
    const match = direct || keyword;
    if (match) {
      this.state.ssicCategory = match.id;
      this.state.ssic = undefined;
    } else if (/^\d+$/.test(value)) {
      this.state.ssic = value;
      this.state.ssicCategory = undefined;
    } else {
      this.state.ssic = undefined;
      this.state.ssicCategory = undefined;
    }
    this.onChange();
  }

  onChange(): void {
    this.stateChange.emit({ ...this.state });
  }
}
