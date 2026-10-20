// backend/api/src/models/job.model.js (Excerpt)

import mongoose from 'mongoose';

const jobSchema = new mongoose.Schema({
  jobId: { type: String, required: true, unique: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  type: { type: String, enum: ['clip', 'caption', 'process-url', 'upload'], required: true },
  status: { 
    type: String, 
    enum: ['queued', 'running', 'completed', 'failed', 'cancelled'], 
    default: 'queued' 
  },
  tempFiles: [{ type: String }], // Array of temporary file paths for cleanup
  result: { type: Object },
  error: { type: String },
}, { timestamps: true });

export const Job = mongoose.model('Job', jobSchema);
