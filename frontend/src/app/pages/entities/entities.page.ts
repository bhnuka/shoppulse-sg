import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService, EntityDetail, EntitySummary } from '../../core/api.service';

@Component({
  selector: 'app-entities-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './entities.page.html',
  styleUrl: './entities.page.css'
})
export class EntitiesPage {
  query = '';
  status = '';
  ssic = '';
  items: EntitySummary[] = [];
  total = 0;
  selected?: EntityDetail;
  loading = false;

  constructor(private readonly api: ApiService) {
    this.search();
  }

  search(): void {
    this.loading = true;
    this.api.searchEntities({
      q: this.query,
      ssic: this.ssic,
      status: this.status,
      limit: 50,
      offset: 0
    }).subscribe((data) => {
      this.items = data.items;
      this.total = data.total;
      this.loading = false;
    });
  }

  select(item: EntitySummary): void {
    this.api.getEntity(item.uen).subscribe((detail) => {
      this.selected = detail;
    });
  }

  close(): void {
    this.selected = undefined;
  }
}
