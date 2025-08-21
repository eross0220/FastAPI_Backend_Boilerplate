import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Data Pipeline Builder',
  description: 'Beautiful UI for CSV file upload and real-time stream logs',
  icons: {
    icon: 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAxUlEQVR4AWLwySwEtF/GNhCDQBCswUW4C7qgDndCK18CRVDIpaT8BvvSBs5sluBBmnjmjOQTS/m/gB2Ac4ICAoRbnkAFHQzQ3ZN/fnISLvkBLhHbAxJoKwN0eiWcd+8P4N0XENMCOGG6Iav89QBOlyipoCkqnhWQKRoPiCefvYJhD5CfSvcHyPQrA7LIvQGySseqgAvE9ACcQzhBoryBMSVAZBlcpBDd428SKueEFHmI24XhDtC7ddMB5YsDwg3lbb+MdsAXgpwpRTasO3oAAAAASUVORK5CYII='
  }
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
          {children}
        </div>
      </body>
    </html>
  )
}
