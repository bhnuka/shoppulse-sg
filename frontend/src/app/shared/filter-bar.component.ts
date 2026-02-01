import { Component, EventEmitter, Input, Output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';

export interface FilterState {
  from?: string;
  to?: string;
  ssic?: string;
  area?: string;
}

@Component({
  selector: 'app-filter-bar',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './filter-bar.component.html',
  styleUrl: './filter-bar.component.css'
})
export class FilterBarComponent {
  @Input() state: FilterState = {};
  @Output() stateChange = new EventEmitter<FilterState>();

  onChange(): void {
    this.stateChange.emit({ ...this.state });
  }
}
