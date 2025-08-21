# CSV Upload & Stream Logs UI

A beautiful, responsive Next.js application for CSV file upload with real-time stream logs display.

## Features

### 🎨 Beautiful UI
- Modern, clean design with Tailwind CSS
- Responsive layout that works on all devices
- Smooth animations and transitions
- Professional color scheme and typography

### 📁 CSV Upload
- Drag & drop file upload
- File validation and error handling
- Real-time processing feedback
- Support for large CSV files
- Automatic parsing with PapaParse

### 📊 Data Preview
- Interactive table view of uploaded data
- Pagination for large datasets
- Export functionality
- Column sorting and filtering

### 📝 Stream Logs
- Real-time log streaming
- Multiple log levels (info, success, warning, error)
- Filter by log type
- Auto-scroll functionality
- Export logs to text file
- Clear logs option

### 🔧 Technical Features
- TypeScript for type safety
- Next.js 14 with App Router
- Responsive grid layout
- Component-based architecture
- Modern React patterns (hooks, callbacks)

## Getting Started

### Prerequisites
- Node.js 18+ 
- npm or yarn

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd csv-upload-logs-ui
```

2. Install dependencies:
```bash
npm install
```

3. Run the development server:
```bash
npm run dev
```

4. Open [http://localhost:3000](http://localhost:3000) in your browser.

### Build for Production

```bash
npm run build
npm start
```

## Usage

1. **Upload CSV File**: Drag and drop a CSV file onto the upload area or click to browse
2. **View Data**: The uploaded data will appear in a table format below the upload area
3. **Monitor Logs**: Watch real-time logs on the right side as the file is processed
4. **Filter Logs**: Use the filter buttons to view specific log types
5. **Export Data**: Export the processed data or logs using the download buttons

## Project Structure

```
├── app/
│   ├── globals.css          # Global styles and Tailwind imports
│   ├── layout.tsx           # Root layout component
│   └── page.tsx             # Main page component
├── components/
│   ├── ui/                  # Reusable UI components
│   │   ├── Button.tsx       # Button component
│   │   └── Card.tsx         # Card component
│   ├── CSVUpload.tsx        # CSV upload component
│   ├── StreamLogs.tsx       # Stream logs component
│   └── DataPreview.tsx      # Data preview component
├── package.json             # Dependencies and scripts
├── tailwind.config.js       # Tailwind CSS configuration
├── tsconfig.json           # TypeScript configuration
└── README.md               # Project documentation
```

## Technologies Used

- **Next.js 14** - React framework with App Router
- **TypeScript** - Type-safe JavaScript
- **Tailwind CSS** - Utility-first CSS framework
- **PapaParse** - CSV parsing library
- **Lucide React** - Beautiful icons
- **clsx** - Conditional className utility

## Customization

### Colors
Modify the color scheme in `tailwind.config.js`:

```javascript
theme: {
  extend: {
    colors: {
      primary: {
        // Your custom primary colors
      }
    }
  }
}
```

### Styling
Custom styles can be added in `app/globals.css`:

```css
@layer components {
  .your-custom-class {
    @apply your-tailwind-classes;
  }
}
```

## Browser Support

- Chrome (latest)
- Firefox (latest)
- Safari (latest)
- Edge (latest)

## License

MIT License - feel free to use this project for your own purposes.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

---

Built with ❤️ using Next.js and Tailwind CSS
