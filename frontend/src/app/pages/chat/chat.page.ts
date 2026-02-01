import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ApiService, ChatResponse } from '../../core/api.service';

interface ChatMessage {
  role: 'user' | 'assistant';
  text: string;
}

@Component({
  selector: 'app-chat-page',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat.page.html',
  styleUrl: './chat.page.css'
})
export class ChatPage {
  question = '';
  messages: ChatMessage[] = [];
  response?: ChatResponse;
  loading = false;

  constructor(private readonly api: ApiService) {}

  submit(): void {
    const question = this.question.trim();
    if (!question) {
      return;
    }
    this.messages.push({ role: 'user', text: question });
    this.loading = true;
    this.api.chatQuery(question).subscribe((res) => {
      this.response = res;
      this.messages.push({ role: 'assistant', text: res.narrative });
      this.loading = false;
    });
    this.question = '';
  }
}
