# DD Bot Professional Dashboard

A colorful, professional dashboard with glowing UI/UX design for managing your DD Bot operations.

## 🎨 Features

### Visual Design
- **Glassmorphism Effects**: Modern frosted glass containers with backdrop blur
- **Gradient Backgrounds**: Beautiful purple-blue-pink color scheme
- **Glowing Elements**: Interactive hover effects and animated borders
- **Smooth Animations**: Transitions, loading states, and success animations
- **Responsive Design**: Works perfectly on desktop and mobile devices

### Dashboard Components
- **Main Dashboard**: Central overview with all module statistics
- **Message List**: Enhanced target management with stats grid
- **Post Queue**: Content scheduling with progress rings
- **Inbox Activity**: Three-tab interface for message monitoring
- **Message History**: Performance tracking with success metrics

### UI Components
- **Stats Grids**: Visual metrics with colorful cards
- **Progress Rings**: Circular progress indicators
- **Status Badges**: Color-coded status indicators
- **Glowing Containers**: Boxed layouts with hover effects
- **Modern Buttons**: Gradient buttons with icons and animations

## 🚀 Quick Start

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the main dashboard
streamlit run main_dashboard.py

# Or run individual pages
streamlit run pages/1_MsgList.py
streamlit run pages/2_PostQueue.py
streamlit run pages/3_InboxActivity.py
streamlit run pages/4_MsgHistory.py
```

### GitHub Pages Deployment

#### Option 1: Using Streamlit Cloud (Recommended)
1. Push your code to GitHub
2. Go to [Streamlit Cloud](https://share.streamlit.io/)
3. Connect your GitHub repository
4. Deploy `main_dashboard.py` as the main file

#### Option 2: Static HTML Export
```bash
# Export to static HTML (requires additional setup)
pip install streamlit-static
streamlit-static export main_dashboard.py
```

#### Option 3: GitHub Pages with Streamlit
1. Enable GitHub Pages in your repository settings
2. Use GitHub Actions to build and deploy
3. Add workflow file in `.github/workflows/deploy.yml`

## 📁 Project Structure

```
DD-Msg-Bot/
├── main_dashboard.py          # Main dashboard entry point
├── pages/
│   ├── 1_MsgList.py          # Message list with enhanced UI
│   ├── 2_PostQueue.py        # Post queue with progress rings
│   ├── 3_InboxActivity.py    # Inbox with three-tab interface
│   └── 4_MsgHistory.py       # History with success metrics
├── styles/
│   └── dashboard.css         # Custom CSS styling
├── utils/
│   └── ui_helpers.py         # Reusable UI components
└── main.py                   # Core bot functionality
```

## 🎨 Customization

### Colors
The main color scheme uses:
- Primary: `#667eea` (Purple)
- Secondary: `#764ba2` (Deep Purple)
- Accent: `#f093fb` (Pink)
- Success: `#66bb6a` (Green)
- Warning: `#ffa726` (Orange)
- Error: `#ef5350` (Red)

### Modifying Styles
Edit `styles/dashboard.css` to customize:
- Colors and gradients
- Animation speeds
- Border radius and shadows
- Typography and spacing

### Adding New Components
Use functions from `utils/ui_helpers.py`:
- `create_glowing_container()` - Boxed content areas
- `create_metric_card()` - Statistics cards
- `create_progress_ring()` - Circular progress
- `create_status_badge()` - Status indicators

## 🔧 Configuration

### Environment Variables
```bash
# Google Sheets Configuration
DD_SHEET_ID=your_sheet_id
CREDENTIALS_FILE=credentials.json

# Bot Configuration
DD_LOGIN_EMAIL=your_email
DD_LOGIN_PASS=your_password
DD_HEADLESS=1
```

### Google Sheets Setup
1. Create a Google Cloud Project
2. Enable Google Sheets API
3. Create service account credentials
4. Share your sheet with the service account email

## 📱 Mobile Support

The dashboard is fully responsive and works on:
- Desktop browsers (Chrome, Firefox, Safari, Edge)
- Tablet devices
- Mobile phones (iOS and Android)

## 🌐 Browser Compatibility

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+

## 🚨 Troubleshooting

### Common Issues
1. **CSS not loading**: Ensure `styles/dashboard.css` exists
2. **Sheets connection**: Check credentials and sheet sharing
3. **Streamlit errors**: Verify all dependencies are installed

### Debug Mode
Enable debug logging:
```bash
export DD_DEBUG=1
streamlit run main_dashboard.py
```

## 📄 License

This project is part of the DD Bot ecosystem. See main repository for license information.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test the dashboard
5. Submit a pull request

## 📞 Support

For issues and questions:
- Create an issue on GitHub
- Check the troubleshooting section
- Review the main project documentation

---

**Enjoy your beautiful new dashboard! 🎉**
